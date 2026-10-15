package io.github.ahmadbitaarr.raftkv.rpc;

import io.grpc.stub.StreamObserver;
import io.github.ahmadbitaarr.raftkv.config.ClusterConfigTest;
import io.github.ahmadbitaarr.raftkv.server.*;
import io.github.ahmadbitaarr.raftkv.rpc.generated.replication.v1.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class ReplicationGrpcObserverFailureTest {
    @TempDir Path dir;

    @Test
    void throwingObserverDoesNotReceiveErrorAfterOnNext() {
        var config = ClusterConfigTest.config(dir, "n2");
        var snapshot = new NodeStatusSnapshot(
                "n2", io.github.ahmadbitaarr.raftkv.server.NodeRole.FOLLOWER,
                NodeLifecycleState.CONTROL_ONLY, 0, 0, 0, false);

        var service = new ReplicationGrpcService(config, () -> snapshot);

        var request = ReplicateEntryRequest.newBuilder()
                .setLeaderId("n1")
                .setEntry(LogEntry.newBuilder()
                        .setIndex(1)
                        .setRequestId("request")
                        .setOperation(OperationType.OPERATION_TYPE_PUT).setValue(com.google.protobuf.ByteString.copyFromUtf8("test-value"))
                        .setKey("key")
                        .setCommandFingerprint(
                                com.google.protobuf.ByteString.copyFromUtf8("x")))
                .build();

        AtomicInteger nextCalls = new AtomicInteger();
        AtomicInteger errorCalls = new AtomicInteger();
        AtomicInteger completedCalls = new AtomicInteger();

        StreamObserver<ReplicateEntryResponse> observer =
                new StreamObserver<>() {
                    @Override
                    public void onNext(ReplicateEntryResponse response) {
                        nextCalls.incrementAndGet();
                        throw new IllegalStateException("observer failed");
                    }

                    @Override
                    public void onError(Throwable error) {
                        errorCalls.incrementAndGet();
                    }

                    @Override
                    public void onCompleted() {
                        completedCalls.incrementAndGet();
                    }
                };

        assertThrows(IllegalStateException.class, () -> service.replicateEntry(request, observer));
        System.out.println("Observer callbacks: next=" + nextCalls.get() + ", error=" + errorCalls.get() + ", completed=" + completedCalls.get());

        assertEquals(1, nextCalls.get());
        assertEquals(0, errorCalls.get(),
                "must not call onError after onNext throws");
        assertEquals(0, completedCalls.get(),
                "must not complete after onNext throws");
    }
}
