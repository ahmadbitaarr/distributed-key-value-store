package io.github.ahmadbitaarr.raftkv.config;

import org.junit.jupiter.api.Test;
import java.nio.file.Path;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class Phase12ComposeConfigTest {
    @Test void allThreeComposeNodeConfigurationsValidateAndAgree() throws Exception {
        var configs=List.of(
                ConfigLoader.load(Path.of("docker/config/node-1.properties")),
                ConfigLoader.load(Path.of("docker/config/node-2.properties")),
                ConfigLoader.load(Path.of("docker/config/node-3.properties")));
        assertEquals(List.of("n1","n2","n3"),configs.stream().map(ClusterConfig::localNodeId).toList());
        assertEquals(1,configs.stream().map(ClusterConfig::topologyDigest).distinct().count());
        for(var c:configs){
            assertEquals("raftkv-compose",c.clusterId());
            assertEquals("n1",c.configuredLeaderId());
            assertEquals(50051,c.localBindPort());
            assertEquals("0.0.0.0",c.localBindHost());
            assertEquals("raftkv.wal",c.walFile().getFileName().toString());
            assertEquals(3,c.members().size());
        }
    }
}
