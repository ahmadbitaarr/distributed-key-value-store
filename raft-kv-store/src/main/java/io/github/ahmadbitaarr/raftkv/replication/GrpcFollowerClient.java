package io.github.ahmadbitaarr.raftkv.replication;

import io.grpc.*;
import io.github.ahmadbitaarr.raftkv.config.ClusterConfig;
import io.github.ahmadbitaarr.raftkv.config.NodeEndpoint;
import io.github.ahmadbitaarr.raftkv.log.LogEntry;
import io.github.ahmadbitaarr.raftkv.rpc.ProtoMapper;
import io.github.ahmadbitaarr.raftkv.rpc.ReplicationContractValidator;
import io.github.ahmadbitaarr.raftkv.rpc.generated.replication.v1.*;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/** Bounded, single-request gRPC client; no retries and no assumption timeout means absent. */
public final class GrpcFollowerClient implements FollowerMaintenanceClient {
    private final ClusterConfig config;
    private final Function<String, NodeEndpoint> endpointResolver;
    private final Map<String, ManagedChannel> channels=new ConcurrentHashMap<>();
    private final Object lifecycleLock = new Object();
    private final Map<String,NodeEndpoint> channelEndpoints=new ConcurrentHashMap<>();
    private final java.util.List<ManagedChannel> retiredChannels=new java.util.ArrayList<>();
    private volatile boolean closed;

    public GrpcFollowerClient(ClusterConfig config) { this(config, id -> config.members().get(id)); }

    /** Package-private seam for ephemeral loopback ports in tests. */
    GrpcFollowerClient(ClusterConfig config, Function<String, NodeEndpoint> endpointResolver) {
        this.config=Objects.requireNonNull(config);
        this.endpointResolver=Objects.requireNonNull(endpointResolver);
        if(config.localRole()!=io.github.ahmadbitaarr.raftkv.server.NodeRole.LEADER)
            throw new IllegalArgumentException("outbound replication requires the configured leader");
    }

    @Override public Outcome replicate(String followerId, LogEntry entry) {
        Objects.requireNonNull(entry);
        if(followerId == null || followerId.equals(config.configuredLeaderId()) || !config.members().containsKey(followerId))
            throw new IllegalArgumentException("unconfigured follower target");
        if(closed) return new Outcome(FollowerClient.Kind.REMOTE_ERROR, entry.index(), 0, 0, "client closed");
        try {
            var request=ReplicateEntryRequest.newBuilder()
                    .setLeaderId(config.configuredLeaderId())
                    .setEntry(ProtoMapper.toProtoLogEntry(entry))
                    .setLeaderCommitIndex(0) // Explicit AdvanceCommit RPC alone propagates commitment.
                    .build();
            ReplicationContractValidator.validate(request);
            ManagedChannel channel=channelFor(followerId);
            ReplicateEntryResponse response=ReplicationServiceGrpc.newBlockingStub(channel)
                    .withDeadlineAfter(config.replicationDeadlineMs(),TimeUnit.MILLISECONDS)
                    .replicateEntry(request);
            try {
                ReplicationContractValidator.validate(response,request);
                if(!followerId.equals(response.getFollowerId()))
                    throw new IllegalArgumentException("wrong follower identity");
            } catch (IllegalArgumentException malformed) {
                return new Outcome(FollowerClient.Kind.INVALID_RESPONSE,entry.index(),0,0,malformed.getMessage());
            }
            if(response.getAccepted()) return FollowerClient.ack(entry.index(),response.getLastPersistedIndex());
            FollowerClient.Kind kind=switch(response.getRejectReason()) {
                case REPLICATION_REJECT_REASON_INDEX_GAP -> FollowerClient.Kind.INDEX_GAP;
                case REPLICATION_REJECT_REASON_INDEX_CONFLICT -> FollowerClient.Kind.INDEX_CONFLICT;
                case REPLICATION_REJECT_REASON_NOT_READY -> FollowerClient.Kind.NOT_READY;
                default -> FollowerClient.Kind.INVALID_RESPONSE;
            };
            return new Outcome(kind,entry.index(),response.getLastPersistedIndex(),
                    response.hasExpectedNextIndex()?response.getExpectedNextIndex():0,response.getMessage());
        } catch (StatusRuntimeException rpc) {
            FollowerClient.Kind kind=switch(rpc.getStatus().getCode()) {
                case DEADLINE_EXCEEDED -> FollowerClient.Kind.DEADLINE_EXCEEDED;
                case UNAVAILABLE -> FollowerClient.Kind.UNAVAILABLE;
                default -> FollowerClient.Kind.REMOTE_ERROR;
            };
            return new Outcome(kind,entry.index(),0,0,rpc.getStatus().getCode().name());
        } catch (RuntimeException unexpected) {
            return new Outcome(FollowerClient.Kind.REMOTE_ERROR,entry.index(),0,0,unexpected.getClass().getSimpleName());
        }
    }

    /** Fresh-follower bootstrap probe on the SAME lifecycle-owned channel as replication. */
    @Override public Optional<FollowerStatusProbe.Status> probeStatus(String followerId) {
        if (followerId == null || followerId.equals(config.configuredLeaderId())
                || !config.members().containsKey(followerId))
            throw new IllegalArgumentException("unconfigured follower target");
        try {
            ManagedChannel channel=channelFor(followerId);
            var request = ReplicationStatusRequest.newBuilder()
                    .setLeaderId(config.configuredLeaderId()).build();
            ReplicationContractValidator.validate(request);
            var response = ReplicationServiceGrpc.newBlockingStub(channel)
                    .withDeadlineAfter(config.statusDeadlineMs(), TimeUnit.MILLISECONDS)
                    .getReplicationStatus(request);
            ReplicationContractValidator.validate(response);
            if (!followerId.equals(response.getNodeId())
                    || response.getRole() != io.github.ahmadbitaarr.raftkv.rpc.generated.replication.v1.NodeRole.NODE_ROLE_FOLLOWER)
                return Optional.empty();
            return Optional.of(new FollowerStatusProbe.Status(followerId,
                    io.github.ahmadbitaarr.raftkv.server.NodeRole.FOLLOWER,
                    response.getLastLogIndex(), response.getCommitIndex(),
                    response.getAppliedIndex(), response.getReady()));
        } catch (io.grpc.StatusRuntimeException | IllegalArgumentException failure) {
            // Unavailable, invalid or unauthenticated metadata can never bootstrap prefix zero.
            return Optional.empty();
        }
    }

    /** Same synchronized registration and owned channel as replication/status. */
    private ManagedChannel channelFor(String followerId) {
        if(followerId==null || followerId.equals(config.configuredLeaderId())
                || !config.members().containsKey(followerId))
            throw new IllegalArgumentException("unconfigured follower target");
        synchronized(lifecycleLock) {
            if(closed) throw new IllegalStateException("follower client closed");
            NodeEndpoint endpoint=Objects.requireNonNull(endpointResolver.apply(followerId),"configured endpoint");
            ManagedChannel existing=channels.get(followerId);
            if(existing!=null && endpoint.equals(channelEndpoints.get(followerId)))return existing;
            if(existing!=null) {
                // Previously in-flight RPCs may still complete; shutdown is graceful.
                existing.shutdown();
                retiredChannels.add(existing);
            }
            ManagedChannel fresh=ManagedChannelBuilder.forAddress(endpoint.host(),endpoint.port()).usePlaintext()
                    .maxInboundMessageSize(config.maxInboundMessageBytes()).build();
            channels.put(followerId,fresh);
            channelEndpoints.put(followerId,endpoint);
            return fresh;
        }
    }

    private static FollowerMaintenanceClient.Result maintenanceError(RuntimeException error) {
        if(error instanceof StatusRuntimeException rpc) {
            return switch(rpc.getStatus().getCode()) {
                case DEADLINE_EXCEEDED -> FollowerMaintenanceClient.Result.failure(FollowerMaintenanceClient.Kind.TIMEOUT,"deadline exceeded; remote outcome unknown");
                case UNAVAILABLE -> FollowerMaintenanceClient.Result.failure(FollowerMaintenanceClient.Kind.UNAVAILABLE,"unavailable");
                case FAILED_PRECONDITION, INVALID_ARGUMENT -> FollowerMaintenanceClient.Result.failure(FollowerMaintenanceClient.Kind.REJECTED,rpc.getStatus().toString());
                default -> FollowerMaintenanceClient.Result.failure(FollowerMaintenanceClient.Kind.FAILED,rpc.getStatus().toString());
            };
        }
        return FollowerMaintenanceClient.Result.failure(FollowerMaintenanceClient.Kind.FAILED,error.toString());
    }

    @Override public FollowerMaintenanceClient.Result advanceCommit(String followerId,long committedIndex) {
        if(committedIndex<0)throw new IllegalArgumentException("negative commit");
        try {
            ManagedChannel channel=channelFor(followerId);
            var request=AdvanceCommitRequest.newBuilder().setLeaderId(config.configuredLeaderId())
                    .setCommitIndex(committedIndex).build();
            ReplicationContractValidator.validate(request);
            var response=ReplicationServiceGrpc.newBlockingStub(channel)
                    .withDeadlineAfter(config.replicationDeadlineMs(),TimeUnit.MILLISECONDS).advanceCommit(request);
            try {
                ReplicationContractValidator.validate(response);
                if(!followerId.equals(response.getFollowerId()) || response.getCommitIndex()<committedIndex
                        || response.getAppliedIndex()<committedIndex)
                    return FollowerMaintenanceClient.Result.failure(FollowerMaintenanceClient.Kind.INVALID_RESPONSE,
                            "wrong follower or incomplete commit/application");
            } catch (IllegalArgumentException malformed) {
                return FollowerMaintenanceClient.Result.failure(FollowerMaintenanceClient.Kind.INVALID_RESPONSE,
                        malformed.getMessage());
            }
            return new FollowerMaintenanceClient.Result(FollowerMaintenanceClient.Kind.ACCEPTED,
                    response.getCommitIndex(),response.getCommitIndex(),response.getAppliedIndex(),"forced and applied");
        } catch(RuntimeException ex) {return maintenanceError(ex);}
    }

    @Override public FollowerMaintenanceClient.Result truncateUncommittedSuffix(String followerId,long retainThrough) {
        if(retainThrough<0)throw new IllegalArgumentException("negative retained index");
        try {
            ManagedChannel channel=channelFor(followerId);
            var request=TruncateSuffixRequest.newBuilder().setLeaderId(config.configuredLeaderId())
                    .setTruncateAfterIndex(retainThrough).build();
            ReplicationContractValidator.validate(request);
            var response=ReplicationServiceGrpc.newBlockingStub(channel)
                    .withDeadlineAfter(config.replicationDeadlineMs(),TimeUnit.MILLISECONDS).truncateUncommittedSuffix(request);
            try {
                ReplicationContractValidator.validate(response);
                if(!followerId.equals(response.getNodeId()) || response.getLastLogIndex()!=retainThrough)
                    return FollowerMaintenanceClient.Result.failure(FollowerMaintenanceClient.Kind.INVALID_RESPONSE,
                            "incorrect post-truncation index/identity");
            } catch (IllegalArgumentException malformed) {
                return FollowerMaintenanceClient.Result.failure(FollowerMaintenanceClient.Kind.INVALID_RESPONSE,
                        malformed.getMessage());
            }
            return new FollowerMaintenanceClient.Result(FollowerMaintenanceClient.Kind.ACCEPTED,
                    response.getLastLogIndex(),response.getCommitIndex(),0,"durable rewrite");
        }catch(RuntimeException ex){return maintenanceError(ex);}
    }

    @Override public synchronized void close() throws IOException {
        synchronized(lifecycleLock) {
            closed=true;
        }
        java.util.List<ManagedChannel> owned;
        synchronized(lifecycleLock) {
            owned=new java.util.ArrayList<>(channels.values());
            owned.addAll(retiredChannels);
        }
        for(ManagedChannel channel:owned) channel.shutdown();
        boolean allTerminated=true;
        for(ManagedChannel channel:owned) {
            try {
                if(!channel.awaitTermination(config.shutdownGraceMs(),TimeUnit.MILLISECONDS)) {
                    channel.shutdownNow();
                    if(!channel.awaitTermination(config.shutdownGraceMs(),TimeUnit.MILLISECONDS)) allTerminated=false;
                }
                if(!channel.isTerminated()) allTerminated=false;
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt(); channel.shutdownNow();
                throw new IOException("interrupted closing gRPC follower channel",interrupted);
            }
        }
        if(!allTerminated) throw new IOException("outbound gRPC channel cleanup incomplete");
        synchronized(lifecycleLock){channels.clear();channelEndpoints.clear();retiredChannels.clear();}
    }
}
