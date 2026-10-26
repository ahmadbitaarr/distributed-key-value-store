package io.github.ahmadbitaarr.raftkv.leader;

import io.github.ahmadbitaarr.raftkv.server.NodeStatusSnapshot;
import io.github.ahmadbitaarr.raftkv.statemachine.StateMachineApplier;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/** Concurrent leader read admission; snapshots are serialized only with state-machine application. */
public final class LeaderReadCoordinator {
    public enum Code { OK, NOT_FOUND, NOT_READY }
    public record ReadResult(Code code,long commitIndex,Optional<byte[]> value) {
        public ReadResult {
            Objects.requireNonNull(code);Objects.requireNonNull(value);
            if((code==Code.NOT_READY && (commitIndex!=0 || value.isPresent()))
                    || (code!=Code.NOT_READY && commitIndex<0)
                    || (code==Code.OK)!=value.isPresent())
                throw new IllegalArgumentException("invalid read outcome");
            value=value.map(bytes -> Arrays.copyOf(bytes,bytes.length));
        }
        @Override public Optional<byte[]> value(){return value.map(bytes -> Arrays.copyOf(bytes,bytes.length));}
    }

    private final StateMachineApplier applier;
    private final Supplier<NodeStatusSnapshot> snapshot;
    private boolean open;
    private int inFlight;

    public LeaderReadCoordinator(StateMachineApplier applier,Supplier<NodeStatusSnapshot> snapshot){
        this.applier=Objects.requireNonNull(applier,"applier");
        this.snapshot=Objects.requireNonNull(snapshot,"snapshot");
    }
    /** Startup-only; admission never reopens after it has been closed. */
    public synchronized void openAfterStartup(){
        if(open || closedOnce)throw new IllegalStateException("read admission cannot reopen");
        open=true;
    }
    private boolean closedOnce;
    public synchronized void closeAdmission(){open=false;closedOnce=true;notifyAll();}
    public synchronized int inFlightReads(){return inFlight;}
    public synchronized boolean awaitDrained(long timeout,TimeUnit unit) throws InterruptedException {
        long remaining=unit.toNanos(timeout);
        long deadline=System.nanoTime()+remaining;
        while(inFlight>0){
            if(remaining<=0)return false;
            TimeUnit.NANOSECONDS.timedWait(this,remaining);
            remaining=deadline-System.nanoTime();
        }
        return true;
    }
    private synchronized Permit tryAcquire(){
        if(!open)return null;
        ++inFlight;
        return new Permit();
    }
    private final class Permit implements AutoCloseable {
        private boolean released;
        @Override public void close(){
            synchronized(LeaderReadCoordinator.this){
                if(!released){released=true;--inFlight;LeaderReadCoordinator.this.notifyAll();}
            }
        }
    }

    public ReadResult read(String key) {
        Objects.requireNonNull(key,"key");
        var permit=tryAcquire();
        if(permit==null)return new ReadResult(Code.NOT_READY,0,Optional.empty());
        try(permit){
            var committed=applier.readCommitted(key,snapshot);
            if(committed.isEmpty())return new ReadResult(Code.NOT_READY,0,Optional.empty());
            var result=committed.orElseThrow();
            return new ReadResult(result.value().isPresent()?Code.OK:Code.NOT_FOUND,
                    result.commitIndex(),result.value());
        }
    }
}
