package de.zannagh.armorhider.client.net;

import de.zannagh.armorhider.ArmorHider;
import de.zannagh.armorhider.util.ExponentialBackoff;
import de.zannagh.eunomia.networking.CommunicationManager;
import de.zannagh.eunomia.networking.PacketType;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;

/**
 * Client-to-server send gate, rebuilt on eunomia's capability handshake. Replaces the old
 * {@code ClientPacketSender}.
 * <p>
 * Armor Hider does not blindly emit custom payloads: sending an unknown channel to a vanilla (or
 * otherwise non-Armor-Hider) server can get the client disconnected. So every outgoing packet is held
 * until eunomia's {@link CommunicationManager#serverCapabilities() serverCapabilities()} probe
 * resolves. Packets sent before that decision are queued; once the probe resolves
 * {@link de.zannagh.eunomia.networking.handshake.ServerCapabilities#isPresent() present} they are
 * flushed in order, and if it resolves absent (or the local {@link #TIMEOUT_MILLIS} backstop elapses
 * with no resolution) they are dropped for the rest of the connection. {@link #reset()} clears this
 * state on every connect and disconnect, so a reconnect always re-tests the new server.
 * <p>
 * All sends run under {@link #MONITOR}, so they are strictly ordered - a fast-path send can never
 * overtake the flush of packets that were queued before the probe resolved.
 * <p>
 * The probe itself (HELLO on join, and the primary timeout) is driven by the eunomia mod's own client
 * wiring; this gate only observes {@code serverCapabilities()} and keeps a backstop timeout so a queue
 * can never be held forever if that resolution never arrives.
 */
public final class ClientSendGate {

    /** Backstop: how long to wait for the capability probe to resolve before dropping queued sends. */
    private static final long TIMEOUT_MILLIS = 10_000L;

    // Guards all of the mutable gate state below.
    private static final Object MONITOR = new Object();
    // Sends queued while the capability decision is still pending.
    private static final Deque<Runnable> pending = new ArrayDeque<>();
    private static boolean installed = false;
    private static boolean waiterRunning = false;
    // Bumped on every reset() so a waiter left over from a previous connection can't drop a fresh queue.
    private static long epoch = 0;

    private ClientSendGate() {
    }

    /**
     * Wires the gate to eunomia's capability resolution. Registers a persistent {@code onResolved}
     * listener that flushes (present) or drops (absent) the queue. Idempotent; call once at client init.
     */
    public static void install() {
        synchronized (MONITOR) {
            if (installed) {
                return;
            }
            installed = true;
        }
        // Listeners persist across reconnects and fire again for each new resolution.
        CommunicationManager.serverCapabilities().onResolved(caps -> onResolved());
    }

    /**
     * Forget the current connection's queued sends and re-gate from scratch. Called on both connect
     * and disconnect so reconnecting to a different server never reuses the previous decision.
     */
    public static void reset() {
        synchronized (MONITOR) {
            pending.clear();
            waiterRunning = false;
            epoch++;
        }
    }

    /**
     * Sends {@code data} on {@code type} honoring the capability gate: immediately if the server is
     * known to run the mod, never if it is known not to, or queued (starting the backstop waiter) while
     * the probe is still pending.
     */
    public static <T> void send(PacketType<T> type, T data) {
        synchronized (MONITOR) {
            var caps = CommunicationManager.serverCapabilities();
            if (caps.isResolved()) {
                if (caps.isPresent()) {
                    doSend(type, data);
                } else {
                    ArmorHider.LOGGER.debug("Suppressing outgoing packet {}: server does not run Armor Hider.",
                            type.channelKey());
                }
                return;
            }
            pending.add(() -> doSend(type, data));
            if (!waiterRunning) {
                waiterRunning = true;
                long myEpoch = epoch;
                Thread waiter = new Thread(() -> runWaiter(myEpoch), "armorhider-capability-wait");
                waiter.setDaemon(true);
                waiter.start();
            }
        }
    }

    /** Flushes or drops the queue when eunomia resolves the server's capabilities. */
    private static void onResolved() {
        synchronized (MONITOR) {
            boolean present = CommunicationManager.serverCapabilities().isPresent();
            if (present) {
                // Flush in submission order while holding the lock so no later fast-path send overtakes them.
                for (Runnable action : new ArrayList<>(pending)) {
                    action.run();
                }
            } else {
                ArmorHider.LOGGER.debug("Server does not run Armor Hider; dropping queued outgoing packets.");
            }
            pending.clear();
            waiterRunning = false;
        }
    }

    /** Backstop: if the probe never resolves within {@link #TIMEOUT_MILLIS}, drop the queue. */
    private static void runWaiter(long myEpoch) {
        boolean resolved = awaitResolution(myEpoch);
        synchronized (MONITOR) {
            if (myEpoch != epoch) {
                // reset() ran while we were waiting: a new connection owns the gate now.
                return;
            }
            if (resolved) {
                // onResolved() already flushed/cleared under the lock; nothing to do.
                return;
            }
            ArmorHider.LOGGER.info(
                    "Server capabilities did not resolve within {} ms; suppressing outgoing packets.",
                    TIMEOUT_MILLIS);
            pending.clear();
            waiterRunning = false;
        }
    }

    /** Polls {@code serverCapabilities().isResolved()} until it resolves or the timeout elapses. */
    private static boolean awaitResolution(long myEpoch) {
        if (CommunicationManager.serverCapabilities().isResolved()) {
            return true;
        }
        ExponentialBackoff backoff = new ExponentialBackoff((int) TIMEOUT_MILLIS);
        // shouldContinue() sleeps between attempts (never under MONITOR), so this does not busy-spin.
        while (backoff.shouldContinue()) {
            synchronized (MONITOR) {
                if (myEpoch != epoch) {
                    return true;
                }
            }
            if (CommunicationManager.serverCapabilities().isResolved()) {
                return true;
            }
        }
        return CommunicationManager.serverCapabilities().isResolved();
    }

    private static <T> void doSend(PacketType<T> type, T data) {
        try {
            CommunicationManager.sendToServer(type, data);
        } catch (Exception e) {
            ArmorHider.LOGGER.debug("Failed to send packet {} to server.", type.channelKey(), e);
        }
    }
}
