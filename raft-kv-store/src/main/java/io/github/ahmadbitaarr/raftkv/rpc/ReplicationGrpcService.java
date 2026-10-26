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

/** Follower replication, forced COMMIT/application and guarded suffix rewrite; callback-safe RPC mapping. */
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
        AdvanceCommitResponse response;
        try {
            ReplicationContractValidator.validate(request);
            requireConfiguredLeader(request.getLeaderId());
            if(snapshot.get().role()!=NodeRole.FOLLOWER || handler==null)
                throw Status.FAILED_PRECONDITION.withDescription("not an enabled follower").asRuntimeException();
            var outcome=handler.advanceCommit(request.getLeaderId(),request.getCommitIndex());
            response=AdvanceCommitResponse.newBuilder().setFollowerId(config.localNodeId())
                    .setCommitIndex(outcome.commitIndex()).setAppliedIndex(outcome.appliedIndex()).build();
            ReplicationContractValidator.validate(response);
        } catch(IllegalArgumentException | NullPointerException e) {
            observer.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());return;
        } catch(io.grpc.StatusRuntimeException e) {observer.onError(e);return;
        } catch(FollowerReplicationHandler.UnrecognizedLeaderException | FollowerReplicationHandler.ProtocolRejectedException e) {
            observer.onError(Status.FAILED_PRECONDITION.withDescription(e.getMessage()).asRuntimeException());return;
        } catch(FollowerReplicationHandler.StorageFailedException e) {
            observer.onError(storageFailureStatus(e.getCause()));return;
        } catch(WalCorruptionException e) {observer.onError(storageFailureStatus(e));return;
        } catch(IOException e) {observer.onError(storageFailureStatus(e));return;
        } catch(RuntimeException e) {observer.onError(Status.INTERNAL.withDescription("follower commit handler failed").asRuntimeException());return;}
        observer.onNext(response);
        observer.onCompleted();
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
        TruncateSuffixResponse response;
        try {
            ReplicationContractValidator.validate(request);
            requireConfiguredLeader(request.getLeaderId());
            if(snapshot.get().role()!=NodeRole.FOLLOWER || handler==null)
                throw Status.FAILED_PRECONDITION.withDescription("not an enabled follower").asRuntimeException();
            var outcome=handler.truncateUncommittedSuffix(request.getLeaderId(),request.getTruncateAfterIndex());
            response=TruncateSuffixResponse.newBuilder().setNodeId(config.localNodeId())
                    .setLastLogIndex(outcome.lastLogIndex()).setCommitIndex(outcome.commitIndex()).build();
            ReplicationContractValidator.validate(response);
        } catch(IllegalArgumentException | NullPointerException e) {
            observer.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());return;
        } catch(io.grpc.StatusRuntimeException e) {observer.onError(e);return;
        } catch(FollowerReplicationHandler.UnrecognizedLeaderException | FollowerReplicationHandler.ProtocolRejectedException e) {
            observer.onError(Status.FAILED_PRECONDITION.withDescription(e.getMessage()).asRuntimeException());return;
        } catch(FollowerReplicationHandler.StorageFailedException e) {observer.onError(storageFailureStatus(e.getCause()));return;
        } catch(WalCorruptionException e) {observer.onError(storageFailureStatus(e));return;
        } catch(IOException e) {observer.onError(storageFailureStatus(e));return;
        } catch(RuntimeException e) {observer.onError(Status.INTERNAL.withDescription("follower rewrite handler failed").asRuntimeException());return;}
        observer.onNext(response);
        observer.onCompleted();
    }

    private static io.grpc.StatusRuntimeException storageFailureStatus(Throwable failure) {
        Throwable cursor=failure;
        while(cursor!=null) {
            if(cursor instanceof WalCorruptionException)
                return Status.DATA_LOSS.withDescription("confirmed follower WAL corruption; storage admission closed").asRuntimeException();
            cursor=cursor.getCause();
        }
        return Status.INTERNAL.withDescription("follower durable storage operation failed; admission closed").asRuntimeException();
    }
    private void requireConfiguredLeader(String claimed){
        if(!config.configuredLeaderId().equals(claimed))
            throw Status.FAILED_PRECONDITION.withDescription("unrecognized configured leader ID").asRuntimeException();
    }
}
