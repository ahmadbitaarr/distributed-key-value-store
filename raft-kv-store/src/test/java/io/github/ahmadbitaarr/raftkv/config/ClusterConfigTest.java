package io.github.ahmadbitaarr.raftkv.config;

import io.github.ahmadbitaarr.raftkv.server.NodeRole;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

public class ClusterConfigTest {
    @TempDir Path dir;
    public static Map<String,NodeEndpoint> members() {
        Map<String,NodeEndpoint> m=new LinkedHashMap<>();
        m.put("n1",new NodeEndpoint("127.0.0.1",50101));
        m.put("n2",new NodeEndpoint("127.0.0.1",50102));
        m.put("n3",new NodeEndpoint("127.0.0.1",50103));
        return m;
    }
    public static ClusterConfig config(Path dir,String local) {
        return new ClusterConfig("cluster",local,"n1",members(),"127.0.0.1",members().get(local).port(),dir,2000,1500,1000,2000,12582912);
    }
    @Test void derivesLeaderOnlyFromConfiguredLeaderId(){assertEquals(NodeRole.LEADER,config(dir,"n1").localRole());}
    @Test void derivesFollowersWithoutPromotion(){assertEquals(NodeRole.FOLLOWER,config(dir,"n2").localRole());assertEquals(NodeRole.FOLLOWER,config(dir,"n3").localRole());}
    @Test void leaderHintUsesAdvertisedEndpointNotBindAddress(){var c=config(dir,"n2");assertEquals("127.0.0.1",c.leaderEndpoint().host());assertEquals(50101,c.leaderEndpoint().port());}
    @Test void requiresExactlyThreeUniqueMembers(){var m=members();m.remove("n3");assertThrows(IllegalArgumentException.class,()->make(m,"n1","n1",50101));}
    @Test void rejectsLeaderNotInMembership(){assertThrows(IllegalArgumentException.class,()->make(members(),"n1","missing",50101));}
    @Test void rejectsLocalIdNotInMembership(){assertThrows(IllegalArgumentException.class,()->make(members(),"missing","n1",50101));}
    @Test void rejectsDuplicateAdvertisedEndpoints(){var m=members();m.put("n3",m.get("n1"));assertThrows(IllegalArgumentException.class,()->make(m,"n1","n1",50101));}
    @Test void rejectsInvalidHostAndPort(){assertThrows(IllegalArgumentException.class,()->new NodeEndpoint("0.0.0.0",1234));assertThrows(IllegalArgumentException.class,()->new NodeEndpoint("example.com",0));}
    @Test void rejectsInconsistentLocalBindPort(){assertThrows(IllegalArgumentException.class,()->make(members(),"n1","n1",9999));}
    @Test void rejectsInvalidDeadlinesAndMessageBudget(){assertThrows(IllegalArgumentException.class,()->new ClusterConfig("cluster","n1","n1",members(),"127.0.0.1",50101,dir,0,1500,1000,2000,12582912));assertThrows(IllegalArgumentException.class,()->new ClusterConfig("cluster","n1","n1",members(),"127.0.0.1",50101,dir,2000,1500,1000,2000,1024));}
    @Test void configurationIsImmutable(){var m=members();var c=make(m,"n1","n1",50101);m.clear();assertEquals(3,c.members().size());assertThrows(UnsupportedOperationException.class,()->c.members().clear());}
    @Test void canonicalTopologyDigestIgnoresLocalOnlyFields(){assertEquals(config(dir.resolve("a"),"n1").topologyDigest(),config(dir.resolve("b"),"n2").topologyDigest());}
    private ClusterConfig make(Map<String,NodeEndpoint> m,String local,String leader,int port){return new ClusterConfig("cluster",local,leader,m,"127.0.0.1",port,dir,2000,1500,1000,2000,12582912);}
}
