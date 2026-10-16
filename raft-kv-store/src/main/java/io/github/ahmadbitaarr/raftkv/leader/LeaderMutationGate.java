package io.github.ahmadbitaarr.raftkv.leader;

import java.util.concurrent.TimeUnit;

/** One-flight admission + shutdown closure under a single independent monitor. */
public final class LeaderMutationGate {
    private boolean open, active;
    public synchronized void open() {
        if(open || active)throw new IllegalStateException("leader mutation gate cannot reopen");
        open=true;
    }
    public synchronized Permit tryAcquire() {
        if(!open || active)return null;
        active=true;return new Permit();
    }
    public synchronized void closeAdmission(){open=false;notifyAll();}
    public synchronized boolean isActive(){return active;}
    public synchronized boolean awaitDrained(long timeout, TimeUnit unit) throws InterruptedException {
        long remaining=unit.toNanos(timeout);
        long deadline=System.nanoTime()+remaining;
        while(active){
            if(remaining<=0)return false;
            TimeUnit.NANOSECONDS.timedWait(this,remaining);
            remaining=deadline-System.nanoTime();
        }
        return true;
    }
    public final class Permit implements AutoCloseable {
        private boolean released;
        @Override public void close() {
            synchronized(LeaderMutationGate.this) {
                if(!released){released=true;active=false;LeaderMutationGate.this.notifyAll();}
            }
        }
    }
}
