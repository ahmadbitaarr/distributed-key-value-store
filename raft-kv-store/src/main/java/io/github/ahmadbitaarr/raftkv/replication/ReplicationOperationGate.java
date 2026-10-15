package io.github.ahmadbitaarr.raftkv.replication;

import java.util.concurrent.TimeUnit;

/** Private, atomic admission/count boundary, independent of NodeServer lifecycle synchronization. */
public final class ReplicationOperationGate {
    private boolean open;
    private int active;

    public synchronized void open() {
        if (open || active != 0) throw new IllegalStateException("replication gate already opened or in use");
        open = true;
    }

    /** Returns null if admission has closed; acquisition and counting are indivisible. */
    public synchronized Permit tryAcquire() {
        if (!open) return null;
        active = Math.addExact(active, 1);
        return new Permit();
    }

    public synchronized void closeAdmission() { open = false; notifyAll(); }

    public synchronized int activeCount() { return active; }

    public synchronized boolean awaitDrained(long timeout, TimeUnit unit) throws InterruptedException {
        long remaining = unit.toNanos(timeout);
        long deadline = System.nanoTime() + remaining;
        while (active != 0) {
            if (remaining <= 0) return false;
            TimeUnit.NANOSECONDS.timedWait(this, remaining);
            remaining = deadline - System.nanoTime();
        }
        return true;
    }

    public final class Permit implements AutoCloseable {
        private boolean released;
        @Override public void close() {
            synchronized (ReplicationOperationGate.this) {
                if (!released) {
                    released = true;
                    active--;
                    ReplicationOperationGate.this.notifyAll();
                }
            }
        }
    }
}
