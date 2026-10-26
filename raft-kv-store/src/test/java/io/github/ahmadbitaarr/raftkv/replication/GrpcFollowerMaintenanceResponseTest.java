package io.github.ahmadbitaarr.raftkv.replication;

import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import io.github.ahmadbitaarr.raftkv.config.ClusterConfigTest;
import io.github.ahmadbitaarr.raftkv.config.NodeEndpoint;
import io.github.ahmadbitaarr.raftkv.rpc.generated.replication.v1.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReference;
import static org.junit.jupiter.api.Assertions.*;

/** F04: maintenance reply identity and index validation across actual outbound gRPC. */
class GrpcFollowerMaintenanceResponseTest {
    @TempDir Path directory;
    private final AtomicReference<AdvanceCommitResponse> commitReply = new AtomicReference<>();
    private final AtomicReference<TruncateSuffixResponse> truncateReply = new AtomicReference<>();

    private static final class Fixture implements AutoCloseable {
        final Server server;
        final GrpcFollowerClient client;
        Fixture(Path dir, AtomicReference<AdvanceCommitResponse> commit,
                AtomicReference<TruncateSuffixResponse> truncate) throws Exception {
            server=NettyServerBuilder.forPort(0)
                    .addService(new ReplicationServiceGrpc.ReplicationServiceImplBase() {
                        @Override public void advanceCommit(AdvanceCommitRequest request,
                                StreamObserver<AdvanceCommitResponse> observer) {
                            observer.onNext(commit.get());observer.onCompleted();
                        }
                        @Override public void truncateUncommittedSuffix(TruncateSuffixRequest request,
                                StreamObserver<TruncateSuffixResponse> observer) {
                            observer.onNext(truncate.get());observer.onCompleted();
                        }
                    }).build().start();
            client=new GrpcFollowerClient(ClusterConfigTest.config(dir,"n1"),
                    ignored -> new NodeEndpoint("127.0.0.1",server.getPort()));
        }
        @Override public void close() throws Exception {
            try {client.close();} finally {server.shutdownNow();}
        }
    }

    @Test void wrongFollowerAndUndercommittedRepliesCannotReportSuccessfulCommit() throws Exception {
        try(var rig=new Fixture(directory,commitReply,truncateReply)) {
            commitReply.set(AdvanceCommitResponse.newBuilder().setFollowerId("n3")
                    .setCommitIndex(2).setAppliedIndex(2).build());
            assertEquals(FollowerMaintenanceClient.Kind.INVALID_RESPONSE,
                    rig.client.advanceCommit("n2",2).kind());
            commitReply.set(AdvanceCommitResponse.newBuilder().setFollowerId("n2")
                    .setCommitIndex(1).setAppliedIndex(1).build());
            assertEquals(FollowerMaintenanceClient.Kind.INVALID_RESPONSE,
                    rig.client.advanceCommit("n2",2).kind());
            commitReply.set(AdvanceCommitResponse.newBuilder().setFollowerId("n2")
                    .setCommitIndex(2).setAppliedIndex(3).build());
            assertEquals(FollowerMaintenanceClient.Kind.INVALID_RESPONSE,
                    rig.client.advanceCommit("n2",2).kind(),"malformed applied > commit cannot certify progress");
        }
    }

    @Test void wrongFollowerOrInvalidIndexReplyCannotCertifyDurableTruncation() throws Exception {
        try(var rig=new Fixture(directory,commitReply,truncateReply)) {
            truncateReply.set(TruncateSuffixResponse.newBuilder().setNodeId("n3")
                    .setLastLogIndex(1).setCommitIndex(0).build());
            assertEquals(FollowerMaintenanceClient.Kind.INVALID_RESPONSE,
                    rig.client.truncateUncommittedSuffix("n2",1).kind());
            truncateReply.set(TruncateSuffixResponse.newBuilder().setNodeId("n2")
                    .setLastLogIndex(2).setCommitIndex(0).build());
            assertEquals(FollowerMaintenanceClient.Kind.INVALID_RESPONSE,
                    rig.client.truncateUncommittedSuffix("n2",1).kind());
            truncateReply.set(TruncateSuffixResponse.newBuilder().setNodeId("n2")
                    .setLastLogIndex(1).setCommitIndex(2).build());
            assertEquals(FollowerMaintenanceClient.Kind.INVALID_RESPONSE,
                    rig.client.truncateUncommittedSuffix("n2",1).kind());
        }
    }
}
