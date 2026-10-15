package io.github.ahmadbitaarr.raftkv.config;

import io.github.ahmadbitaarr.raftkv.rpc.RpcLimits;
import io.github.ahmadbitaarr.raftkv.server.NodeRole;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.stream.Collectors;

/** One immutable view of the statically configured three-member cluster. */
public final class ClusterConfig {
    private final String clusterId, localNodeId, configuredLeaderId, localBindHost;
    private final Map<String,NodeEndpoint> members;
    private final int localBindPort, clientDeadlineMs, replicationDeadlineMs, statusDeadlineMs, shutdownGraceMs, maxInboundMessageBytes;
    private final Path walDirectory;

    public ClusterConfig(String clusterId, String localNodeId, String configuredLeaderId,
            Map<String,NodeEndpoint> members, String localBindHost, int localBindPort, Path walDirectory,
            int clientDeadlineMs, int replicationDeadlineMs, int statusDeadlineMs,
            int shutdownGraceMs, int maxInboundMessageBytes) {
        text(clusterId,128,"cluster.id");text(localNodeId,RpcLimits.MAX_NODE_ID_BYTES,"local.node.id");
        text(configuredLeaderId,RpcLimits.MAX_NODE_ID_BYTES,"cluster.leader.id");
        this.clusterId=clusterId;this.localNodeId=localNodeId;this.configuredLeaderId=configuredLeaderId;
        Objects.requireNonNull(members,"members");
        if(members.size()!=3) throw new IllegalArgumentException("exactly three cluster members required");
        Map<String,NodeEndpoint> copy=new TreeMap<>();
        Set<NodeEndpoint> endpoints=new HashSet<>();
        members.forEach((id,ep)->{
            text(id,RpcLimits.MAX_NODE_ID_BYTES,"member ID");
            Objects.requireNonNull(ep,"member endpoint");
            if(!endpoints.add(ep)) throw new IllegalArgumentException("duplicate advertised endpoint");
            copy.put(id,ep);
        });
        if(!copy.containsKey(localNodeId) || !copy.containsKey(configuredLeaderId))
            throw new IllegalArgumentException("local and leader IDs must belong to cluster");
        this.members=Collections.unmodifiableMap(copy);
        this.localBindHost=Objects.requireNonNull(localBindHost,"bind host");
        if(!localBindHost.matches("[A-Za-z0-9][A-Za-z0-9.-]*") || localBindHost.isBlank() || localBindHost.contains(".."))
            throw new IllegalArgumentException("invalid bind host");
        if(localBindPort<1 || localBindPort>65535 || copy.get(localNodeId).port()!=localBindPort)
            throw new IllegalArgumentException("bind port must equal local advertised port");
        this.localBindPort=localBindPort;
        this.walDirectory=Objects.requireNonNull(walDirectory,"wal directory").toAbsolutePath().normalize();
        if(this.walDirectory.getFileName()==null) throw new IllegalArgumentException("WAL directory must not be filesystem root");
        this.clientDeadlineMs=deadline(clientDeadlineMs,"client");
        this.replicationDeadlineMs=deadline(replicationDeadlineMs,"replication");
        this.statusDeadlineMs=deadline(statusDeadlineMs,"status");
        this.shutdownGraceMs=deadline(shutdownGraceMs,"shutdown");
        if(maxInboundMessageBytes!=RpcLimits.SERVER_INBOUND_MESSAGE_BYTES)
            throw new IllegalArgumentException("inbound message budget must equal 12 MiB");
        this.maxInboundMessageBytes=maxInboundMessageBytes;
    }
    private static int deadline(int n,String label) {if(n<1 || n>60_000)throw new IllegalArgumentException(label+" timeout must be 1..60000ms");return n;}
    private static void text(String s,int limit,String label){RpcLimits.validText(s,limit,label,true);}
    public String clusterId(){return clusterId;}
    public String localNodeId(){return localNodeId;}
    public String configuredLeaderId(){return configuredLeaderId;}
    public Map<String,NodeEndpoint> members(){return members;}
    public String localBindHost(){return localBindHost;}
    public int localBindPort(){return localBindPort;}
    public Path walDirectory(){return walDirectory;}
    public Path walFile(){return walDirectory.resolve("raftkv.wal");}
    public int clientDeadlineMs(){return clientDeadlineMs;}
    public int replicationDeadlineMs(){return replicationDeadlineMs;}
    public int statusDeadlineMs(){return statusDeadlineMs;}
    public int shutdownGraceMs(){return shutdownGraceMs;}
    public int maxInboundMessageBytes(){return maxInboundMessageBytes;}
    public NodeEndpoint leaderEndpoint(){return members.get(configuredLeaderId);}
    public NodeRole localRole(){return localNodeId.equals(configuredLeaderId)?NodeRole.LEADER:NodeRole.FOLLOWER;}
    /** Only diagnostic/config-fixture comparison; does not provide peer attestation. */
    public String topologyDigest(){
        String canonical="raftkv-topology-v1\n"+clusterId+"\n"+configuredLeaderId+"\n"+
                members.entrySet().stream().map(e->e.getKey()+"="+e.getValue().host()+":"+e.getValue().port()+"\n")
                    .collect(Collectors.joining());
        try{
            byte[] bytes=MessageDigest.getInstance("SHA-256").digest(canonical.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(bytes);
        }catch(NoSuchAlgorithmException ex){throw new IllegalStateException("SHA-256 unavailable",ex);}
    }
}
