package io.github.ahmadbitaarr.raftkv.server;

import java.util.Objects;
/** One atomic, immutable view of recovered metadata; does not grant mutation/read permission. */
public record NodeStatusSnapshot(String localNodeId,NodeRole role,NodeLifecycleState state,
        long lastLogIndex,long durableCommitIndex,long appliedIndex,boolean ready) {
    public NodeStatusSnapshot {
        Objects.requireNonNull(localNodeId,"localNodeId");Objects.requireNonNull(role,"role");Objects.requireNonNull(state,"state");
        if(localNodeId.isBlank() || lastLogIndex<0 || durableCommitIndex<0 || appliedIndex<0
           || appliedIndex>durableCommitIndex || durableCommitIndex>lastLogIndex)
            throw new IllegalArgumentException("invalid node status indexes or identity");
        if(ready || state==NodeLifecycleState.READY)throw new IllegalArgumentException("Phase 5 cannot be data ready");
    }
    public NodeStatusSnapshot withState(NodeLifecycleState next){return new NodeStatusSnapshot(localNodeId,role,next,lastLogIndex,durableCommitIndex,appliedIndex,false);}
}
