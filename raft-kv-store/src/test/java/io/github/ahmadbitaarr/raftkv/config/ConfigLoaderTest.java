package io.github.ahmadbitaarr.raftkv.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.*;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ConfigLoaderTest {

    @TempDir
    Path dir;

    private String props(String local, int port, Path wal) {
        return "cluster.id=cluster\n"
                + "cluster.leader.id=n1\n"
                + "cluster.member.n1=127.0.0.1:50101\n"
                + "cluster.member.n2=127.0.0.1:50102\n"
                + "cluster.member.n3=127.0.0.1:50103\n"
                + "local.node.id=" + local + "\n"
                + "server.bind.host=127.0.0.1\n"
                + "server.bind.port=" + port + "\n"
                + "storage.wal.directory=" + wal.toString().replace("\\", "\\\\") + "\n"
                + "rpc.client.deadline.ms=2000\n"
                + "rpc.replication.deadline.ms=1500\n"
                + "rpc.status.deadline.ms=1000\n"
                + "server.shutdown.grace.ms=2000\n"
                + "rpc.max.inbound.bytes=12582912\n";
    }

    private Path file(String text) throws Exception {
        Path f = dir.resolve("config.properties");
        Files.writeString(f, text);
        return f;
    }

    @Test
    void loadsValidThreeNodeProperties() throws Exception {
        assertEquals(
                "n2",
                ConfigLoader.load(file(props("n2", 50102, dir.resolve("wal")))).localNodeId()
        );
    }

    @Test
    void selectsCliPathBeforeEnvironmentPath() {
        assertEquals(
                Path.of("a").toAbsolutePath().normalize(),
                ConfigLoader.resolvePath(
                        new String[]{"--config=a"},
                        Map.of("RAFTKV_CONFIG", "b")
                )
        );
    }

    @Test
    void requiresConfiguredPath() {
        assertThrows(
                IllegalArgumentException.class,
                () -> ConfigLoader.resolvePath(new String[0], Map.of())
        );
    }

    @Test
    void rejectsMissingOrUnknownMandatoryProperties() throws Exception {
        assertThrows(
                IllegalArgumentException.class,
                () -> ConfigLoader.load(
                        file(props("n1", 50101, dir.resolve("w")) + "unexpected.key=yes\n")
                )
        );
    }

    @Test
    void rejectsDuplicateProperties() throws Exception {
        assertThrows(
                IllegalArgumentException.class,
                () -> ConfigLoader.load(
                        file(props("n1", 50101, dir.resolve("w")) + "cluster.leader.id=n2\n")
                )
        );
    }

    @Test
    void rejectsMalformedNumericProperties() throws Exception {
        assertThrows(
                IllegalArgumentException.class,
                () -> ConfigLoader.load(
                        file(props("n1", 50101, dir.resolve("w"))
                                .replace("server.bind.port=50101", "server.bind.port=NaN"))
                )
        );
    }

    @Test
    void rejectsInvalidWalDirectory() throws Exception {
        Path parent = dir.resolve("occupied");
        Files.writeString(parent, "not dir");

        assertThrows(
                Exception.class,
                () -> ConfigLoader.load(
                        file(props("n1", 50101, parent.resolve("child")))
                )
        );
    }

    @Test
    void threeIndependentConfigFixturesShareTopology() throws Exception {
        var a = ConfigLoader.load(file(props("n1", 50101, dir.resolve("w1"))));
        var b = ConfigLoader.load(file(props("n2", 50102, dir.resolve("w2"))));

        assertEquals(a.topologyDigest(), b.topologyDigest());
    }

    @Test
    void detectsDivergentLeaderOrEndpointFixture() throws Exception {
        var a = ConfigLoader.load(file(props("n1", 50101, dir.resolve("w1"))));
        var b = ConfigLoader.load(
                file(props("n2", 50102, dir.resolve("w2"))
                        .replace("cluster.leader.id=n1", "cluster.leader.id=n2"))
        );

        assertNotEquals(a.topologyDigest(), b.topologyDigest());
    }

    @Test
    void detectsDuplicateWalDirectoriesAcrossFixtureSet() throws Exception {
        var a = ConfigLoader.load(file(props("n1", 50101, dir.resolve("same"))));
        var b = ConfigLoader.load(file(props("n2", 50102, dir.resolve("same"))));

        assertEquals(a.walDirectory(), b.walDirectory());
    }
}