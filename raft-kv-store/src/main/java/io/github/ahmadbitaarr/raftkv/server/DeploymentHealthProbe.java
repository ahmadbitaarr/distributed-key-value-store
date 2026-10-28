package io.github.ahmadbitaarr.raftkv.server;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.github.ahmadbitaarr.raftkv.rpc.ClientContractValidator;
import io.github.ahmadbitaarr.raftkv.rpc.ReplicationContractValidator;
import io.github.ahmadbitaarr.raftkv.rpc.generated.client.v1.*;
import io.github.ahmadbitaarr.raftkv.rpc.generated.replication.v1.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** Read-only container health check using existing protocol (never opens the WAL). */
public final class DeploymentHealthProbe {
    public static final String RESERVED_KEY = "__raftkv_deployment_health_probe__";
    private DeploymentHealthProbe() { }

    public record Options(String role, String node, String leader, int port) {
        public Options {
            if (!role.equals("leader") && !role.equals("follower")) throw new IllegalArgumentException("invalid role");
            if (!node.matches("[A-Za-z0-9_-]{1,80}") || !leader.matches("[A-Za-z0-9_-]{1,80}"))
                throw new IllegalArgumentException("invalid identity");
            if (port<1 || port>65535) throw new IllegalArgumentException("invalid port");
            if (role.equals("leader") != node.equals(leader)) throw new IllegalArgumentException("role/identity mismatch");
        }
    }
    public static Options parse(String[] args) {
        Map<String,String> values = new HashMap<>();
        for (String arg:args) {
            if (!arg.startsWith("--") || !arg.contains("=")) throw new IllegalArgumentException("expected --name=value");
            String[] parts=arg.substring(2).split("=",2);
            if (!java.util.Set.of("role","node","leader","port").contains(parts[0]) ||
                    values.putIfAbsent(parts[0],parts[1])!=null) throw new IllegalArgumentException("unknown/duplicate option");
        }
        if (values.size()!=4) throw new IllegalArgumentException("--role --node --leader --port required");
        return new Options(values.get("role"),values.get("node"),values.get("leader"),Integer.parseInt(values.get("port")));
    }
    public static boolean leaderReady(GetResponse reply) {
        if (reply==null || (reply.getCode()!=ResultCode.RESULT_CODE_OK && reply.getCode()!=ResultCode.RESULT_CODE_NOT_FOUND)) return false;
        try { ClientContractValidator.validate(reply, GetRequest.newBuilder().setKey(RESERVED_KEY).build()); return true; }
        catch (RuntimeException malformed) { return false; }
    }
    public static boolean followerHealthy(ReplicationStatusResponse reply, String expectedNode) {
        if (reply==null || expectedNode==null || !expectedNode.equals(reply.getNodeId())
                || reply.getRole()!=io.github.ahmadbitaarr.raftkv.rpc.generated.replication.v1.NodeRole.NODE_ROLE_FOLLOWER
                || reply.getReady() || reply.getAppliedIndex()<0 || reply.getCommitIndex()<reply.getAppliedIndex()
                || reply.getLastLogIndex()<reply.getCommitIndex()) return false;
        try { ReplicationContractValidator.validate(reply); return true; }
        catch (RuntimeException malformed) { return false; }
    }
    public static boolean probe(Options options) {
        ManagedChannel channel=ManagedChannelBuilder.forAddress("127.0.0.1",options.port()).usePlaintext().build();
        try {
            if (options.role().equals("leader")) {
                GetRequest request=GetRequest.newBuilder().setKey(RESERVED_KEY).build();
                return leaderReady(KeyValueServiceGrpc.newBlockingStub(channel).withDeadlineAfter(2,TimeUnit.SECONDS).get(request));
            }
            ReplicationStatusRequest request=ReplicationStatusRequest.newBuilder().setLeaderId(options.leader()).build();
            return followerHealthy(ReplicationServiceGrpc.newBlockingStub(channel).withDeadlineAfter(2,TimeUnit.SECONDS)
                    .getReplicationStatus(request),options.node());
        } catch (RuntimeException unavailable) { return false; }
        finally {
            channel.shutdownNow();
            try { channel.awaitTermination(1,TimeUnit.SECONDS); }
            catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
        }
    }
    public static void main(String[] args) {
        try {
            if (!probe(parse(args))) System.exit(1);
        } catch (RuntimeException invalid) {
            System.err.println("health probe failed: "+invalid.getMessage());
            System.exit(2);
        }
    }
}
