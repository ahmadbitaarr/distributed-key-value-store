package io.github.ahmadbitaarr.raftkv.client;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.github.ahmadbitaarr.raftkv.config.NodeEndpoint;
import io.github.ahmadbitaarr.raftkv.log.Command;
import io.github.ahmadbitaarr.raftkv.log.OperationType;
import io.github.ahmadbitaarr.raftkv.rpc.ClientContractValidator;
import io.github.ahmadbitaarr.raftkv.rpc.generated.client.v1.*;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/** Lifecycle-owned blocking gRPC adapter for the bounded client retry policy. */
public final class GrpcMutationTransport implements RetryingMutationClient.Transport, AutoCloseable {
    // Channel publication and shutdown share one ownership boundary.
    // No RPC is performed while holding this monitor.
    private final Object channelLock = new Object();
    private final Map<NodeEndpoint,ManagedChannel> channels = new HashMap<>();
    private final Function<NodeEndpoint,ManagedChannel> channelFactory;
    private volatile boolean closed;

    public GrpcMutationTransport() {
        this(e -> ManagedChannelBuilder.forAddress(e.host(),e.port()).usePlaintext().build());
    }

    // Test seam for deterministic channel construction / close races.
    GrpcMutationTransport(Function<NodeEndpoint,ManagedChannel> channelFactory) {
        this.channelFactory = Objects.requireNonNull(channelFactory);
    }
    @Override public RetryingMutationClient.Response send(NodeEndpoint endpoint,String requestId,
            Command command,long remainingDeadlineMs) {
        Objects.requireNonNull(endpoint);Objects.requireNonNull(requestId);Objects.requireNonNull(command);
        ManagedChannel channel;
        synchronized(channelLock) {
            if(closed)return unavailable();
            channel=channels.get(endpoint);
            if(channel==null) {
                channel=Objects.requireNonNull(channelFactory.apply(endpoint),"channelFactory returned null");
                channels.put(endpoint,channel);
            }
        }
        // close() may begin as soon as we release the monitor; every RPC is
        // already deadline-bounded and a closed channel cannot report false OK.
        if(closed)return unavailable();
        var stub=KeyValueServiceGrpc.newBlockingStub(channel).withDeadlineAfter(Math.max(1,remainingDeadlineMs),TimeUnit.MILLISECONDS);
        MutationResponse reply;
        try {
            if(command.type()==OperationType.PUT){
                PutRequest req=PutRequest.newBuilder().setRequestId(requestId).setKey(command.key())
                        .setValue(ByteString.copyFrom(command.value().orElseThrow())).build();
                ClientContractValidator.validate(req);
                reply=stub.put(req);
                ClientContractValidator.validate(reply,req);
            }else if(command.type()==OperationType.DELETE){
                DeleteRequest req=DeleteRequest.newBuilder().setRequestId(requestId).setKey(command.key()).build();
                ClientContractValidator.validate(req);
                reply=stub.delete(req);
                ClientContractValidator.validate(reply,req);
            }else throw new IllegalArgumentException("unsupported mutation");
        }catch(StatusRuntimeException remote) {
            if(remote.getStatus().getCode()==Status.Code.DEADLINE_EXCEEDED)
                return new RetryingMutationClient.Response(RetryingMutationClient.Code.DEADLINE_EXCEEDED,0,null);
            if(remote.getStatus().getCode()==Status.Code.UNAVAILABLE)
                return new RetryingMutationClient.Response(RetryingMutationClient.Code.UNAVAILABLE,0,null);
            return new RetryingMutationClient.Response(RetryingMutationClient.Code.UNKNOWN_OUTCOME,0,null);
        }catch(RuntimeException invalidReply) {
            // Invalid/mismatched replies can never be silently promoted to committed success.
            return new RetryingMutationClient.Response(RetryingMutationClient.Code.UNKNOWN_OUTCOME,0,null);
        }
        var code=switch(reply.getCode()) {
            case RESULT_CODE_OK -> RetryingMutationClient.Code.OK;
            case RESULT_CODE_REQUEST_ID_CONFLICT -> RetryingMutationClient.Code.REQUEST_ID_CONFLICT;
            case RESULT_CODE_NOT_LEADER -> RetryingMutationClient.Code.NOT_LEADER;
            case RESULT_CODE_NOT_READY -> RetryingMutationClient.Code.NOT_READY;
            case RESULT_CODE_NO_QUORUM -> RetryingMutationClient.Code.NO_QUORUM;
            default -> RetryingMutationClient.Code.UNKNOWN_OUTCOME;
        };
        return new RetryingMutationClient.Response(code,code==RetryingMutationClient.Code.OK?reply.getCommitIndex():0,
                reply.hasLeaderHint()?reply.getLeaderHint().getLeaderId():null);
    }
    private static RetryingMutationClient.Response unavailable() {
        return new RetryingMutationClient.Response(RetryingMutationClient.Code.UNAVAILABLE,0,null);
    }
    @Override public void close(){
        synchronized(channelLock) {
            if(closed)return;
            closed=true;
            for(var channel:channels.values())channel.shutdownNow();
            channels.clear();
        }
        // All pending blocking RPCs have an explicit deadline. No mutation is assumed rolled back.
    }
}
