package io.github.ahmadbitaarr.raftkv.client;

import io.github.ahmadbitaarr.raftkv.config.NodeEndpoint;
import io.github.ahmadbitaarr.raftkv.log.Command;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/** Minimal retry policy/router; transport and bounded backoff are injected for deterministic tests. */
public final class RetryingMutationClient {
    public enum Code { OK, REQUEST_ID_CONFLICT, NOT_LEADER, NOT_READY, NO_QUORUM,
        UNAVAILABLE, DEADLINE_EXCEEDED, UNKNOWN_OUTCOME }
    public record Response(Code code,long commitIndex,String hintedLeaderId) {
        public Response {
            Objects.requireNonNull(code);
            if((code==Code.OK)!=(commitIndex>0))throw new IllegalArgumentException("invalid response index");
        }
    }
    public record Result(String requestId,Code code,long commitIndex,int attempts) { }
    public record Policy(int maxAttempts,long deadlineMs,long initialBackoffMs,long maxBackoffMs,
                         boolean retryNoQuorum) {
        public Policy {
            if(maxAttempts<1 || maxAttempts>100 || deadlineMs<1 || deadlineMs>600_000
                    || initialBackoffMs<1 || maxBackoffMs<initialBackoffMs || maxBackoffMs>60_000)
                throw new IllegalArgumentException("invalid bounded retry policy");
        }
    }
    @FunctionalInterface public interface Transport {
        Response send(NodeEndpoint target,String requestId,Command command,long remainingDeadlineMs) throws Exception;
    }
    @FunctionalInterface public interface Sleeper {void sleep(long milliseconds) throws InterruptedException;}
    private final Map<String,NodeEndpoint> endpoints;
    private final String originalLeader;
    private final Transport transport;
    private final Policy policy;
    private final LongSupplier nanoTime;
    private final Sleeper sleeper;

    public RetryingMutationClient(Map<String,NodeEndpoint> endpoints,String configuredLeader,
            Transport transport,Policy policy) {
        this(endpoints,configuredLeader,transport,policy,System::nanoTime,Thread::sleep);
    }
    public RetryingMutationClient(Map<String,NodeEndpoint> endpoints,String configuredLeader,
            Transport transport,Policy policy,LongSupplier nanoTime,Sleeper sleeper) {
        this.endpoints=Map.copyOf(Objects.requireNonNull(endpoints));
        this.originalLeader=Objects.requireNonNull(configuredLeader);
        if(!this.endpoints.containsKey(originalLeader))throw new IllegalArgumentException("leader not configured");
        this.transport=Objects.requireNonNull(transport);this.policy=Objects.requireNonNull(policy);
        this.nanoTime=Objects.requireNonNull(nanoTime);this.sleeper=Objects.requireNonNull(sleeper);
    }
    public Result mutate(Command command){return mutate(UUID.randomUUID().toString(),command);}
    public Result mutate(String id,Command command){
        if(id==null || id.isBlank())throw new IllegalArgumentException("invalid request ID");
        Objects.requireNonNull(command);
        // Command owns its bytes. The exact instance and ID are kept for all attempts.
        String target=originalLeader;
        long deadline=nanoTime.getAsLong()+TimeUnit.MILLISECONDS.toNanos(policy.deadlineMs());
        long backoff=policy.initialBackoffMs();
        int attempts=0;
        while(attempts<policy.maxAttempts()) {
            long remaining=TimeUnit.NANOSECONDS.toMillis(deadline-nanoTime.getAsLong());
            if(remaining<=0)break;
            attempts++;
            Response response;
            try {response=Objects.requireNonNull(transport.send(endpoints.get(target),id,command,remaining));}
            catch(Exception transportFailure){response=new Response(Code.UNAVAILABLE,0,null);}
            if(response.code()==Code.OK || response.code()==Code.REQUEST_ID_CONFLICT)
                return new Result(id,response.code(),response.commitIndex(),attempts);
            switch(response.code()) {
                case NOT_LEADER -> {
                    // This is a static-leader cluster: membership is not leader authority.
                    // Only the configured leader ID and its configured endpoint are trusted.
                    if(originalLeader.equals(response.hintedLeaderId()))
                        target=originalLeader;
                    else return new Result(id,Code.UNKNOWN_OUTCOME,0,attempts);
                }
                case NO_QUORUM -> {
                    if(!policy.retryNoQuorum())return new Result(id,Code.NO_QUORUM,0,attempts);
                }
                case UNAVAILABLE, DEADLINE_EXCEEDED, NOT_READY -> { }
                default -> {return new Result(id,Code.UNKNOWN_OUTCOME,0,attempts);}
            }
            if(attempts>=policy.maxAttempts())break;
            remaining=TimeUnit.NANOSECONDS.toMillis(deadline-nanoTime.getAsLong());
            if(remaining<=0)break;
            try {sleeper.sleep(Math.min(backoff,remaining));}
            catch(InterruptedException interrupted){Thread.currentThread().interrupt();break;}
            backoff=Math.min(policy.maxBackoffMs(),backoff>Long.MAX_VALUE/2 ? Long.MAX_VALUE : backoff*2);
        }
        return new Result(id,Code.UNKNOWN_OUTCOME,0,attempts);
    }
}
