package io.github.ahmadbitaarr.raftkv.statemachine;

import io.github.ahmadbitaarr.raftkv.log.PersistentLog;
import java.util.Objects;
import java.util.Optional;
import java.util.Arrays;
import java.util.function.Supplier;
import io.github.ahmadbitaarr.raftkv.server.NodeLifecycleState;
import io.github.ahmadbitaarr.raftkv.server.NodeRole;
import io.github.ahmadbitaarr.raftkv.server.NodeStatusSnapshot;

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
    /** A defensive, immutable logical read at exactly one committed/applied index. */
    public record CommittedRead(long commitIndex, Optional<byte[]> value) {
        public CommittedRead {
            if(commitIndex<0)throw new IllegalArgumentException("negative commit index");
            Objects.requireNonNull(value,"value");
            value=value.map(bytes -> Arrays.copyOf(bytes,bytes.length));
        }
        @Override public Optional<byte[]> value() {
            return value.map(bytes -> Arrays.copyOf(bytes,bytes.length));
        }
    }

    /** Synchronizes status, application progress, and value with applyThrough. Never repairs a mismatch. */
    public synchronized Optional<CommittedRead> readCommitted(String key,Supplier<NodeStatusSnapshot> published) {
        Objects.requireNonNull(key,"key");
        Objects.requireNonNull(published,"published");
        var state=published.get();
        if(failed || state.state()!=NodeLifecycleState.READY || !state.ready()
                || state.role()!=NodeRole.LEADER || appliedIndex!=state.appliedIndex()
                || appliedIndex!=state.durableCommitIndex())return Optional.empty();
        return Optional.of(new CommittedRead(appliedIndex,machine.get(key)));
    }

    public synchronized java.util.Optional<byte[]> diagnosticGetForTest(String key){return machine.get(key);}
}
