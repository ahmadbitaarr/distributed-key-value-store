package io.github.ahmadbitaarr.raftkv.rpc;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.github.ahmadbitaarr.raftkv.config.ClusterConfig;
import io.github.ahmadbitaarr.raftkv.server.NodeRole;
import io.github.ahmadbitaarr.raftkv.server.NodeStatusSnapshot;
import io.github.ahmadbitaarr.raftkv.rpc.generated.replication.v1.*;
import java.util.Objects;
import java.util.function.Supplier;

/** Phase 5 diagnostics and truthful non-ACKs. Does not mutate any WAL. */
public final class ReplicationGrpcService extends ReplicationServiceGrpc.ReplicationServiceImplBase {
    private final ClusterConfig config;
    private final Supplier<NodeStatusSnapshot> snapshot;
    public ReplicationGrpcService(ClusterConfig config,Supplier<NodeStatusSnapshot> snapshot){
        this.config=Objects.requireNonNull(config,"config");this.snapshot=Objects.requireNonNull(snapshot,"snapshot");
    }
    @Override public void replicateEntry(ReplicateEntryRequest request,StreamObserver<ReplicateEntryResponse> observer){
        try{
            ReplicationContractValidator.validate(request);
            requireConfiguredLeader(request.getLeaderId());
            var current=snapshot.get();
            if(current.role()!=NodeRole.FOLLOWER)throw Status.FAILED_PRECONDITION.withDescription("leader is not follower target").asRuntimeException();
            var response=ReplicateEntryResponse.newBuilder().setFollowerId(config.localNodeId()).setAccepted(false)
                    .setLastPersistedIndex(current.lastLogIndex()).setCommitIndex(current.durableCommitIndex())
                    .setRejectReason(ReplicationRejectReason.REPLICATION_REJECT_REASON_NOT_READY).build();
            ReplicationContractValidator.validate(response);
            observer.onNext(response);observer.onCompleted();
        }catch(IllegalArgumentException | NullPointerException e){observer.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());}
        catch(io.grpc.StatusRuntimeException e){observer.onError(e);}
        catch(RuntimeException e){observer.onError(Status.INTERNAL.withDescription("replicate control handler failed").asRuntimeException());}
    }
    @Override public void advanceCommit(AdvanceCommitRequest request,StreamObserver<AdvanceCommitResponse> observer){
        try{
            ReplicationContractValidator.validate(request);requireConfiguredLeader(request.getLeaderId());
            observer.onError(Status.FAILED_PRECONDITION.withDescription("commit advancement disabled until later phase").asRuntimeException());
        }catch(IllegalArgumentException | NullPointerException e){observer.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());}
        catch(io.grpc.StatusRuntimeException e){observer.onError(e);}
    }
    @Override public void getReplicationStatus(ReplicationStatusRequest request,StreamObserver<ReplicationStatusResponse> observer){
        try{
            ReplicationContractValidator.validate(request);requireConfiguredLeader(request.getLeaderId());
            var current=snapshot.get();
            ReplicationStatusResponse response=ReplicationStatusResponse.newBuilder().setNodeId(config.localNodeId())
                    .setRole(current.role()==NodeRole.LEADER?io.github.ahmadbitaarr.raftkv.rpc.generated.replication.v1.NodeRole.NODE_ROLE_LEADER:
                            io.github.ahmadbitaarr.raftkv.rpc.generated.replication.v1.NodeRole.NODE_ROLE_FOLLOWER)
                    .setLastLogIndex(current.lastLogIndex()).setCommitIndex(current.durableCommitIndex())
                    .setAppliedIndex(current.appliedIndex()).setReady(false).build();
            ReplicationContractValidator.validate(response);
            observer.onNext(response);observer.onCompleted();
        }catch(IllegalArgumentException | NullPointerException e){observer.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());}
        catch(io.grpc.StatusRuntimeException e){observer.onError(e);}
        catch(RuntimeException e){observer.onError(Status.INTERNAL.withDescription("status handler failed").asRuntimeException());}
    }
    @Override public void truncateUncommittedSuffix(TruncateSuffixRequest request,StreamObserver<TruncateSuffixResponse> observer){
        try{
            ReplicationContractValidator.validate(request);requireConfiguredLeader(request.getLeaderId());
            observer.onError(Status.FAILED_PRECONDITION.withDescription("suffix truncation disabled until later phase").asRuntimeException());
        }catch(IllegalArgumentException | NullPointerException e){observer.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());}
        catch(io.grpc.StatusRuntimeException e){observer.onError(e);}
    }
    private void requireConfiguredLeader(String claimed){
        if(!config.configuredLeaderId().equals(claimed))
            throw Status.FAILED_PRECONDITION.withDescription("unrecognized configured leader ID").asRuntimeException();
    }
}
