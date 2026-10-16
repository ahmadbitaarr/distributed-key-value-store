package io.github.ahmadbitaarr.raftkv.rpc;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.github.ahmadbitaarr.raftkv.config.ClusterConfig;
import io.github.ahmadbitaarr.raftkv.leader.LeaderCoordinator;
import io.github.ahmadbitaarr.raftkv.log.Command;
import io.github.ahmadbitaarr.raftkv.server.*;
import io.github.ahmadbitaarr.raftkv.rpc.generated.client.v1.*;
import io.github.ahmadbitaarr.raftkv.wal.WalCorruptionException;
import java.io.IOException;
import java.util.Objects;
import java.util.function.Supplier;

/** Validates client requests; leader mutation execution remains in LeaderCoordinator. */
public final class ClientKvGrpcService extends KeyValueServiceGrpc.KeyValueServiceImplBase {
    private final ClusterConfig config;
    private final Supplier<NodeStatusSnapshot> snapshot;
    private final LeaderCoordinator coordinator;

    /** Phase 5/6 compatibility: no leader mutations enabled when collaborator is absent. */
    public ClientKvGrpcService(ClusterConfig config,Supplier<NodeStatusSnapshot> snapshot) {
        this(config,snapshot,null);
    }
    public ClientKvGrpcService(ClusterConfig config,Supplier<NodeStatusSnapshot> snapshot,
                               LeaderCoordinator coordinator) {
        this.config=Objects.requireNonNull(config,"config");
        this.snapshot=Objects.requireNonNull(snapshot,"snapshot");
        this.coordinator=coordinator;
    }

    @Override public void put(PutRequest request,StreamObserver<MutationResponse> observer) {
        try {ClientContractValidator.validate(request);}
        catch(IllegalArgumentException | NullPointerException malformed) {
            observer.onError(Status.INVALID_ARGUMENT.withDescription(malformed.getMessage()).asRuntimeException());return;
        }
        MutationResponse reply;
        try {
            reply=mutation(request.getRequestId(),Command.put(request.getKey(),request.getValue().toByteArray()));
            ClientContractValidator.validate(reply,request);
        } catch(WalCorruptionException corruption) {
            observer.onError(Status.DATA_LOSS.withDescription("leader WAL corruption; write admission disabled").asRuntimeException());return;
        } catch(IOException failed) {
            observer.onError(Status.INTERNAL.withDescription("leader mutation storage/commit/application failed").asRuntimeException());return;
        } catch(RuntimeException failed) {
            observer.onError(Status.INTERNAL.withDescription("leader PUT execution failed").asRuntimeException());return;
        }
        observer.onNext(reply);observer.onCompleted(); // callbacks MUST NOT be inside execution catch
    }
    @Override public void delete(DeleteRequest request,StreamObserver<MutationResponse> observer) {
        try {ClientContractValidator.validate(request);}
        catch(IllegalArgumentException | NullPointerException malformed) {
            observer.onError(Status.INVALID_ARGUMENT.withDescription(malformed.getMessage()).asRuntimeException());return;
        }
        MutationResponse reply;
        try {
            reply=mutation(request.getRequestId(),Command.delete(request.getKey()));
            ClientContractValidator.validate(reply,request);
        } catch(WalCorruptionException corruption) {
            observer.onError(Status.DATA_LOSS.withDescription("leader WAL corruption; write admission disabled").asRuntimeException());return;
        } catch(IOException failed) {
            observer.onError(Status.INTERNAL.withDescription("leader mutation storage/commit/application failed").asRuntimeException());return;
        } catch(RuntimeException failed) {
            observer.onError(Status.INTERNAL.withDescription("leader DELETE execution failed").asRuntimeException());return;
        }
        observer.onNext(reply);observer.onCompleted();
    }
    @Override public void get(GetRequest request,StreamObserver<GetResponse> observer) {
        try {ClientContractValidator.validate(request);}
        catch(IllegalArgumentException | NullPointerException malformed) {
            observer.onError(Status.INVALID_ARGUMENT.withDescription(malformed.getMessage()).asRuntimeException());return;
        }
        GetResponse reply;
        try {
            var current=snapshot.get();
            boolean follower=current.state()==NodeLifecycleState.CONTROL_ONLY && current.role()==NodeRole.FOLLOWER;
            var builder=GetResponse.newBuilder().setKey(request.getKey())
                    .setCode(follower?ResultCode.RESULT_CODE_NOT_LEADER:ResultCode.RESULT_CODE_NOT_READY);
            if(follower)builder.setLeaderHint(leaderHint());
            reply=builder.build();
            ClientContractValidator.validate(reply,request);
        } catch(RuntimeException failed) {
            observer.onError(Status.INTERNAL.withDescription("GET control handler failed").asRuntimeException());return;
        }
        observer.onNext(reply);observer.onCompleted();
    }
    private MutationResponse mutation(String requestId,Command command) throws IOException {
        var current=snapshot.get();
        boolean follower=current.state()==NodeLifecycleState.CONTROL_ONLY && current.role()==NodeRole.FOLLOWER;
        var builder=MutationResponse.newBuilder().setRequestId(requestId);
        if(follower) {
            return builder.setCode(ResultCode.RESULT_CODE_NOT_LEADER).setLeaderHint(leaderHint()).build();
        }
        if(current.state()!=NodeLifecycleState.CONTROL_ONLY || current.role()!=NodeRole.LEADER || coordinator==null)
            return builder.setCode(ResultCode.RESULT_CODE_NOT_READY).build();
        LeaderCoordinator.MutationResult result=coordinator.mutate(requestId,command);
        return switch(result.code()) {
            case OK -> builder.setCode(ResultCode.RESULT_CODE_OK).setCommitIndex(result.commitIndex()).build();
            case NO_QUORUM -> builder.setCode(ResultCode.RESULT_CODE_NO_QUORUM).build();
            case NOT_READY -> builder.setCode(ResultCode.RESULT_CODE_NOT_READY).build();
        };
    }
    private LeaderHint leaderHint() {
        var endpoint=config.leaderEndpoint();
        return LeaderHint.newBuilder().setLeaderId(config.configuredLeaderId())
                .setHost(endpoint.host()).setPort(endpoint.port()).build();
    }
}
