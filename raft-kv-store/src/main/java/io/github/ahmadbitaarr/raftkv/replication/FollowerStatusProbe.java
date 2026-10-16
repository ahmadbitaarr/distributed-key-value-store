package io.github.ahmadbitaarr.raftkv.replication;

import io.github.ahmadbitaarr.raftkv.server.NodeRole;
import java.util.Optional;

/** A bounded read-only status lookup; absence is NOT an empty-prefix certificate. */
@FunctionalInterface
public interface FollowerStatusProbe {
    record Status(String nodeId, NodeRole role, long lastLogIndex, long commitIndex,
                  long appliedIndex, boolean ready) {
        public Status {
            if (nodeId == null || nodeId.isBlank() || role == null || lastLogIndex < 0
                    || appliedIndex < 0 || appliedIndex > commitIndex || commitIndex > lastLogIndex)
                throw new IllegalArgumentException("invalid replication status");
        }
    }
    Optional<Status> probeStatus(String followerId);
}
