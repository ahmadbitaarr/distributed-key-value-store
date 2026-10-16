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
public final class GrpcFollowerClient implements FollowerClient, FollowerStatusProbe {
    private final ClusterConfig config;
    private final Function<String, NodeEndpoint> endpointResolver;
    private final Map<String, ManagedChannel> channels=new ConcurrentHashMap<>();
    private final Object lifecycleLock = new Object();
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
        if(closed) return new Outcome(Kind.REMOTE_ERROR, entry.index(), 0, 0, "client closed");
        try {
            var request=ReplicateEntryRequest.newBuilder()
                    .setLeaderId(config.configuredLeaderId())
                    .setEntry(ProtoMapper.toProtoLogEntry(entry))
                    .setLeaderCommitIndex(0) // Phase 6 never propagates COMMIT.
                    .build();
            ReplicationContractValidator.validate(request);
            ManagedChannel channel;
            synchronized(lifecycleLock) {
                if(closed) return new Outcome(Kind.REMOTE_ERROR,entry.index(),0,0,"client closed");
                channel=channels.computeIfAbsent(followerId, id -> {
                NodeEndpoint ep=Objects.requireNonNull(endpointResolver.apply(id),"configured endpoint");
                return ManagedChannelBuilder.forAddress(ep.host(),ep.port()).usePlaintext()
                        .maxInboundMessageSize(config.maxInboundMessageBytes()).build();
            });
            }
            ReplicateEntryResponse response=ReplicationServiceGrpc.newBlockingStub(channel)
                    .withDeadlineAfter(config.replicationDeadlineMs(),TimeUnit.MILLISECONDS)
                    .replicateEntry(request);
            try {
                ReplicationContractValidator.validate(response,request);
                if(!followerId.equals(response.getFollowerId()))
                    throw new IllegalArgumentException("wrong follower identity");
            } catch (IllegalArgumentException malformed) {
                return new Outcome(Kind.INVALID_RESPONSE,entry.index(),0,0,malformed.getMessage());
            }
            if(response.getAccepted()) return FollowerClient.ack(entry.index(),response.getLastPersistedIndex());
            Kind kind=switch(response.getRejectReason()) {
                case REPLICATION_REJECT_REASON_INDEX_GAP -> Kind.INDEX_GAP;
                case REPLICATION_REJECT_REASON_INDEX_CONFLICT -> Kind.INDEX_CONFLICT;
                case REPLICATION_REJECT_REASON_NOT_READY -> Kind.NOT_READY;
                default -> Kind.INVALID_RESPONSE;
            };
            return new Outcome(kind,entry.index(),response.getLastPersistedIndex(),
                    response.hasExpectedNextIndex()?response.getExpectedNextIndex():0,response.getMessage());
        } catch (StatusRuntimeException rpc) {
            Kind kind=switch(rpc.getStatus().getCode()) {
                case DEADLINE_EXCEEDED -> Kind.DEADLINE_EXCEEDED;
                case UNAVAILABLE -> Kind.UNAVAILABLE;
                default -> Kind.REMOTE_ERROR;
            };
            return new Outcome(kind,entry.index(),0,0,rpc.getStatus().getCode().name());
        } catch (RuntimeException unexpected) {
            return new Outcome(Kind.REMOTE_ERROR,entry.index(),0,0,unexpected.getClass().getSimpleName());
        }
    }

    /** Fresh-follower bootstrap probe on the SAME lifecycle-owned channel as replication. */
    @Override public Optional<FollowerStatusProbe.Status> probeStatus(String followerId) {
        if (followerId == null || followerId.equals(config.configuredLeaderId())
                || !config.members().containsKey(followerId))
            throw new IllegalArgumentException("unconfigured follower target");
        try {
            ManagedChannel channel;
            synchronized (lifecycleLock) {
                if (closed) return Optional.empty();
                channel = channels.computeIfAbsent(followerId, id -> {
                    NodeEndpoint endpoint = Objects.requireNonNull(endpointResolver.apply(id), "configured endpoint");
                    return ManagedChannelBuilder.forAddress(endpoint.host(), endpoint.port()).usePlaintext()
                            .maxInboundMessageSize(config.maxInboundMessageBytes()).build();
                });
            }
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

    @Override public synchronized void close() throws IOException {
        synchronized(lifecycleLock) {
            closed=true;
        }
        for(ManagedChannel channel:channels.values()) channel.shutdown();
        boolean allTerminated=true;
        for(ManagedChannel channel:channels.values()) {
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
        channels.clear();
    }
}
