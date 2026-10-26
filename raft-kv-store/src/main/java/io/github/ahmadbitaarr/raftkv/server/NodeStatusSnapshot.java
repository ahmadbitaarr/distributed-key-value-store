package io.github.ahmadbitaarr.raftkv.server;

import java.util.Objects;
/** One immutable, internally consistent publication of node lifecycle and WAL/application progress. */
public record NodeStatusSnapshot(String localNodeId,NodeRole role,NodeLifecycleState state,
        long lastLogIndex,long durableCommitIndex,long appliedIndex,boolean ready) {
    public NodeStatusSnapshot {
        Objects.requireNonNull(localNodeId,"localNodeId");Objects.requireNonNull(role,"role");Objects.requireNonNull(state,"state");
        if(localNodeId.isBlank() || lastLogIndex<0 || durableCommitIndex<0 || appliedIndex<0
           || appliedIndex>durableCommitIndex || durableCommitIndex>lastLogIndex)
            throw new IllegalArgumentException("invalid node status indexes or identity");
        if(ready != (state==NodeLifecycleState.READY)
                || (ready && role!=NodeRole.LEADER))
            throw new IllegalArgumentException("READY requires the configured leader and ready=true");
    }
    public NodeStatusSnapshot withState(NodeLifecycleState next){return new NodeStatusSnapshot(localNodeId,role,next,lastLogIndex,durableCommitIndex,appliedIndex,next==NodeLifecycleState.READY);}
}
