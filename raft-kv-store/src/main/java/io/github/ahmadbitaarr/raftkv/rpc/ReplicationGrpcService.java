package io.github.ahmadbitaarr.raftkv.rpc;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.github.ahmadbitaarr.raftkv.config.ClusterConfig;
import io.github.ahmadbitaarr.raftkv.server.NodeRole;
import io.github.ahmadbitaarr.raftkv.server.NodeStatusSnapshot;
import io.github.ahmadbitaarr.raftkv.rpc.generated.replication.v1.*;
import java.util.Objects;
import java.io.IOException;
import io.github.ahmadbitaarr.raftkv.wal.WalCorruptionException;
import io.github.ahmadbitaarr.raftkv.replication.FollowerReplicationHandler;
import java.util.function.Supplier;

/** Phase 5 diagnostics and truthful non-ACKs. Does not mutate any WAL. */
public final class ReplicationGrpcService extends ReplicationServiceGrpc.ReplicationServiceImplBase {
    private final ClusterConfig config;
    private final Supplier<NodeStatusSnapshot> snapshot;
    private final FollowerReplicationHandler handler;
    public ReplicationGrpcService(ClusterConfig config,Supplier<NodeStatusSnapshot> snapshot){
        this(config, snapshot, null);
    }
    public ReplicationGrpcService(ClusterConfig config,Supplier<NodeStatusSnapshot> snapshot,
            FollowerReplicationHandler handler){
        this.config=Objects.requireNonNull(config,"config");
        this.snapshot=Objects.requireNonNull(snapshot,"snapshot");
        this.handler=handler;
    }
    @Override public void replicateEntry(ReplicateEntryRequest request,StreamObserver<ReplicateEntryResponse> observer){
        ReplicateEntryResponse response;
        try{
            ReplicationContractValidator.validate(request);
            requireConfiguredLeader(request.getLeaderId());
            var current=snapshot.get();
            if(current.role()!=NodeRole.FOLLOWER)
                throw Status.FAILED_PRECONDITION.withDescription("leader is not follower target").asRuntimeException();
            FollowerReplicationHandler.Decision decision;
            if(handler==null){
                decision=new FollowerReplicationHandler.Decision(FollowerReplicationHandler.Kind.NOT_READY,
                        current.lastLogIndex(),current.durableCommitIndex(),0);
            }else{
                decision=handler.replicate(request.getLeaderId(),ProtoMapper.toDomainLogEntry(request.getEntry()));
            }
            var builder=ReplicateEntryResponse.newBuilder()
                    .setFollowerId(config.localNodeId()).setAccepted(decision.accepted())
                    .setLastPersistedIndex(decision.lastPersistedIndex())
                    .setCommitIndex(decision.commitIndex());
            switch(decision.kind()){
                case ACCEPTED -> { }
                case INDEX_GAP -> builder.setRejectReason(ReplicationRejectReason.REPLICATION_REJECT_REASON_INDEX_GAP)
                                         .setExpectedNextIndex(decision.expectedNextIndex());
                case INDEX_CONFLICT -> builder.setRejectReason(ReplicationRejectReason.REPLICATION_REJECT_REASON_INDEX_CONFLICT)
                                              .setExpectedNextIndex(decision.expectedNextIndex());
                case NOT_READY -> builder.setRejectReason(ReplicationRejectReason.REPLICATION_REJECT_REASON_NOT_READY);
            }
            response=builder.build();
            ReplicationContractValidator.validate(response,request);
        }catch(IllegalArgumentException | NullPointerException e){
            observer.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException()); return;
        }catch(io.grpc.StatusRuntimeException e){observer.onError(e); return;}
        catch(FollowerReplicationHandler.UnrecognizedLeaderException e){
            observer.onError(Status.FAILED_PRECONDITION.withDescription(e.getMessage()).asRuntimeException()); return;
        }catch(FollowerReplicationHandler.StorageFailedException e){
            if (e.getCause() instanceof WalCorruptionException) {
                observer.onError(Status.DATA_LOSS.withDescription("follower WAL corruption; storage admission disabled").asRuntimeException());
            } else {
                observer.onError(Status.FAILED_PRECONDITION.withDescription("follower storage failed").asRuntimeException());
            }
            return;
        }catch(WalCorruptionException e){
            observer.onError(Status.DATA_LOSS.withDescription("follower WAL corruption; storage admission disabled").asRuntimeException()); return;
        }catch(IOException e){
            observer.onError(Status.INTERNAL.withDescription("follower durable WAL append failed").asRuntimeException()); return;
        }catch(RuntimeException e){
            observer.onError(Status.INTERNAL.withDescription("replicate handler failed").asRuntimeException()); return;
        }
        observer.onNext(response);
        observer.onCompleted();
    }
    @Override public void advanceCommit(AdvanceCommitRequest request,StreamObserver<AdvanceCommitResponse> observer){
        io.grpc.StatusRuntimeException rejection;
        try {
            ReplicationContractValidator.validate(request);
            requireConfiguredLeader(request.getLeaderId());
            rejection=Status.FAILED_PRECONDITION.withDescription("commit advancement disabled until later phase").asRuntimeException();
        } catch (IllegalArgumentException | NullPointerException e) {
            rejection=Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException();
        } catch (io.grpc.StatusRuntimeException e) {
            rejection=e;
        }
        // Observer callbacks must not be inside the validation/error-mapping try block.
        observer.onError(rejection);
    }
    @Override public void getReplicationStatus(ReplicationStatusRequest request,StreamObserver<ReplicationStatusResponse> observer){
        ReplicationStatusResponse response;
        try {
            ReplicationContractValidator.validate(request);
            requireConfiguredLeader(request.getLeaderId());
            var current=snapshot.get();
            response=ReplicationStatusResponse.newBuilder().setNodeId(config.localNodeId())
                    .setRole(current.role()==NodeRole.LEADER?io.github.ahmadbitaarr.raftkv.rpc.generated.replication.v1.NodeRole.NODE_ROLE_LEADER:
                            io.github.ahmadbitaarr.raftkv.rpc.generated.replication.v1.NodeRole.NODE_ROLE_FOLLOWER)
                    .setLastLogIndex(current.lastLogIndex()).setCommitIndex(current.durableCommitIndex())
                    .setAppliedIndex(current.appliedIndex()).setReady(current.ready()).build();
            ReplicationContractValidator.validate(response);
        } catch (IllegalArgumentException | NullPointerException e) {
            observer.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());
            return;
        } catch (io.grpc.StatusRuntimeException e) {
            observer.onError(e);
            return;
        } catch (RuntimeException e) {
            observer.onError(Status.INTERNAL.withDescription("status handler failed").asRuntimeException());
            return;
        }
        // Callback failures propagate rather than triggering a second terminal signal.
        observer.onNext(response);
        observer.onCompleted();
    }
    @Override public void truncateUncommittedSuffix(TruncateSuffixRequest request,StreamObserver<TruncateSuffixResponse> observer){
        io.grpc.StatusRuntimeException rejection;
        try {
            ReplicationContractValidator.validate(request);
            requireConfiguredLeader(request.getLeaderId());
            rejection=Status.FAILED_PRECONDITION.withDescription("suffix truncation disabled until later phase").asRuntimeException();
        } catch (IllegalArgumentException | NullPointerException e) {
            rejection=Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException();
        } catch (io.grpc.StatusRuntimeException e) {
            rejection=e;
        }
        observer.onError(rejection);
    }
    private void requireConfiguredLeader(String claimed){
        if(!config.configuredLeaderId().equals(claimed))
            throw Status.FAILED_PRECONDITION.withDescription("unrecognized configured leader ID").asRuntimeException();
    }
}
