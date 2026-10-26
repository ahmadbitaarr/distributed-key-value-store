package io.github.ahmadbitaarr.raftkv.replication;

import java.util.Objects;

/** Internal bounded maintenance RPCs, using the same owned transport as live replication. */
public interface FollowerMaintenanceClient extends FollowerClient, FollowerStatusProbe {
    enum Kind { ACCEPTED, REJECTED, UNAVAILABLE, TIMEOUT, INVALID_RESPONSE, FAILED }
    record Result(Kind kind, long lastLogIndex, long commitIndex, long appliedIndex, String detail) {
        public Result {
            Objects.requireNonNull(kind); Objects.requireNonNull(detail);
            if(lastLogIndex<0 || commitIndex<0 || appliedIndex<0 || appliedIndex>commitIndex)
                throw new IllegalArgumentException("invalid maintenance progress");
        }
        public boolean accepted(){return kind==Kind.ACCEPTED;}
        public static Result failure(Kind kind,String message){return new Result(kind,0,0,0,message);}
    }
    Result advanceCommit(String followerId,long committedIndex);
    Result truncateUncommittedSuffix(String followerId,long retainThrough);
}
