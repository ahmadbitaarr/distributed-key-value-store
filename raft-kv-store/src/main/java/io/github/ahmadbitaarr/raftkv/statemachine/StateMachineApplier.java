package io.github.ahmadbitaarr.raftkv.statemachine;

import io.github.ahmadbitaarr.raftkv.log.PersistentLog;
import java.util.Objects;

/** In-memory ordered leader application, never applies beyond durable COMMIT. */
public final class StateMachineApplier {
    private final KvStateMachine machine;
    private long appliedIndex;
    private boolean failed;
    public StateMachineApplier(KvStateMachine machine) {this.machine=Objects.requireNonNull(machine);}
    public synchronized long appliedIndex(){return appliedIndex;}
    public synchronized boolean failed(){return failed;}
    public synchronized long applyThrough(long target, long durableCommitIndex, PersistentLog log) {
        Objects.requireNonNull(log);
        if(failed) throw new IllegalStateException("state-machine applier failed");
        if(target<appliedIndex || target>durableCommitIndex || durableCommitIndex>log.lastLogIndex())
            throw new IllegalArgumentException("invalid apply/commit/log index ordering");
        while(appliedIndex<target) {
            try {
                long next=appliedIndex+1;
                machine.apply(log.get(next).orElseThrow().command());
                appliedIndex=next; // publish only after state machine finished applying
            } catch(RuntimeException | Error failure) {
                failed=true;
                throw failure;
            }
        }
        return appliedIndex;
    }
    public synchronized java.util.Optional<byte[]> diagnosticGetForTest(String key){return machine.get(key);}
}
