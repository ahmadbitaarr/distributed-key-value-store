package io.github.ahmadbitaarr.raftkv.rpc;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.github.ahmadbitaarr.raftkv.config.ClusterConfig;
import io.github.ahmadbitaarr.raftkv.server.*;
import io.github.ahmadbitaarr.raftkv.rpc.generated.client.v1.*;
import java.util.Objects;
import java.util.function.Supplier;

/** Phase 5 validation and role-aware control-only responses, never a KV executor. */
public final class ClientKvGrpcService extends KeyValueServiceGrpc.KeyValueServiceImplBase {
    private final ClusterConfig config;
    private final Supplier<NodeStatusSnapshot> snapshot;
    public ClientKvGrpcService(ClusterConfig config,Supplier<NodeStatusSnapshot> snapshot){
        this.config=Objects.requireNonNull(config,"config");this.snapshot=Objects.requireNonNull(snapshot,"snapshot");
    }
    @Override public void put(PutRequest request,StreamObserver<MutationResponse> observer) {
        try {
            ClientContractValidator.validate(request);
            var reply=mutation(request.getRequestId());
            ClientContractValidator.validate(reply,request);
            observer.onNext(reply);observer.onCompleted();
        }catch(IllegalArgumentException | NullPointerException e){observer.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());}
        catch(RuntimeException e){observer.onError(Status.INTERNAL.withDescription("PUT control handler failed").asRuntimeException());}
    }
    @Override public void delete(DeleteRequest request,StreamObserver<MutationResponse> observer) {
        try {
            ClientContractValidator.validate(request);
            var reply=mutation(request.getRequestId());
            ClientContractValidator.validate(reply,request);
            observer.onNext(reply);observer.onCompleted();
        }catch(IllegalArgumentException | NullPointerException e){observer.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());}
        catch(RuntimeException e){observer.onError(Status.INTERNAL.withDescription("DELETE control handler failed").asRuntimeException());}
    }
    @Override public void get(GetRequest request,StreamObserver<GetResponse> observer) {
        try {
            ClientContractValidator.validate(request);
            var current=snapshot.get();
            boolean follower=current.state()==NodeLifecycleState.CONTROL_ONLY && current.role()==NodeRole.FOLLOWER;
            var builder=GetResponse.newBuilder().setKey(request.getKey()).setCode(follower ? ResultCode.RESULT_CODE_NOT_LEADER : ResultCode.RESULT_CODE_NOT_READY);
            if(follower)builder.setLeaderHint(leaderHint());
            GetResponse response=builder.build();
            ClientContractValidator.validate(response,request);
            observer.onNext(response);observer.onCompleted();
        }catch(IllegalArgumentException | NullPointerException e){observer.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());}
        catch(RuntimeException e){observer.onError(Status.INTERNAL.withDescription("GET control handler failed").asRuntimeException());}
    }
    private MutationResponse mutation(String requestId) {
        var current=snapshot.get();
        boolean follower=current.state()==NodeLifecycleState.CONTROL_ONLY && current.role()==NodeRole.FOLLOWER;
        var builder=MutationResponse.newBuilder().setRequestId(requestId)
                .setCode(follower ? ResultCode.RESULT_CODE_NOT_LEADER : ResultCode.RESULT_CODE_NOT_READY);
        if(follower) builder.setLeaderHint(leaderHint());
        return builder.build();
    }
    private LeaderHint leaderHint() {
        var endpoint=config.leaderEndpoint();
        return LeaderHint.newBuilder().setLeaderId(config.configuredLeaderId())
                .setHost(endpoint.host()).setPort(endpoint.port()).build();
    }
}
