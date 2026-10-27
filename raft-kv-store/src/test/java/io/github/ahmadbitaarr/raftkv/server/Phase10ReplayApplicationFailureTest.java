package io.github.ahmadbitaarr.raftkv.server;

import com.google.protobuf.ByteString;
import io.github.ahmadbitaarr.raftkv.config.ClusterConfig;
import io.github.ahmadbitaarr.raftkv.config.ClusterConfigTest;
import io.github.ahmadbitaarr.raftkv.leader.CanonicalCommandFingerprint;
import io.github.ahmadbitaarr.raftkv.log.Command;
import io.github.ahmadbitaarr.raftkv.log.LogEntry;
import io.github.ahmadbitaarr.raftkv.log.OperationType;
import io.github.ahmadbitaarr.raftkv.rpc.ClientKvGrpcService;
import io.github.ahmadbitaarr.raftkv.rpc.generated.client.v1.*;
import io.github.ahmadbitaarr.raftkv.wal.*;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

/** B06: an actual committed-entry application fault must prevent recovered-leader data admission. */
class Phase10ReplayApplicationFailureTest {
    @TempDir Path directory;

    private static LogEntry entry(long index, String id, Command command) {
        return new LogEntry(index, id, command, CanonicalCommandFingerprint.of(command));
    }

    /** Capture the actual service response; failed startup must never expose a live listener. */
    private static final class Reply<T> implements StreamObserver<T> {
        private T value;
        private Throwable error;
        private int completions;
        @Override public void onNext(T next) { assertNull(value); value = next; }
        @Override public void onError(Throwable cause) { error = cause; }
        @Override public void onCompleted() { completions++; }
        T completedValue() {
            assertNull(error, "RPC must return its explicit admission-denied result");
            assertEquals(1, completions);
            assertNotNull(value);
            return value;
        }
    }

    @Test void b06FailureDuringCommittedReplayNeverPublishesReadyOrAdmitsGetPut() throws Exception {
        ClusterConfig config = ClusterConfigTest.config(directory, "n1");
        Path path = config.walFile();
        Command first = Command.put("valid", new byte[]{4, 2});
        Command second = Command.delete("poisoned");
        try (var durable = FileWalStore.open(path)) {
            durable.appendEntryDurably(entry(1, "committed-1", first));
            durable.appendEntryDurably(entry(2, "committed-2", second));
            durable.appendCommitDurably(2);
        }

        AtomicBoolean replayFaultInjected = new AtomicBoolean();
        AtomicBoolean listenerCreationAttempted = new AtomicBoolean();
        NodeServer.WalOpener injection = walPath -> {
            // The disk is genuine, valid, forced and committed. Only the decoded in-memory
            // view given to replay is fault-injected, preserving the actual WAL bytes.
            FileWalStore physical = FileWalStore.open(walPath);
            WalRecoveryResult authentic = physical.recoveryResult();
            assertEquals(2, authentic.commitIndex());
            assertEquals(2, authentic.entries().size());

            // Simulate an application-invalid recovered command after successful frame
            // decoding: a PUT without its required value. This cannot be constructed by
            // the public Command API, so alter a throwaway test-only Command instance.
            Command broken = Command.delete("poisoned");
            try {
                Field type = Command.class.getDeclaredField("type");
                type.setAccessible(true);
                type.set(broken, OperationType.PUT);
            } catch (ReflectiveOperationException reflectiveFailure) {
                physical.close();
                throw new IOException("cannot inject recovered-entry application fault", reflectiveFailure);
            }
            assertEquals(OperationType.PUT, broken.type());
            assertTrue(broken.value().isEmpty());

            List<LogEntry> decoded = new ArrayList<>(authentic.entries());
            LogEntry original = decoded.get(1);
            decoded.set(1, new LogEntry(original.index(), original.requestId(), broken,
                    original.commandFingerprint()));
            WalRecoveryResult injected = new WalRecoveryResult(decoded, authentic.commitIndex(),
                    authentic.lastValidOffset(), authentic.originalFileLength(), authentic.tailTruncated());
            replayFaultInjected.set(true);
            return new WalStore() {
                @Override public WalRecoveryResult recoveryResult() { return injected; }
                @Override public void appendEntryDurably(LogEntry value) throws IOException {
                    physical.appendEntryDurably(value);
                }
                @Override public void appendCommitDurably(long index) throws IOException {
                    physical.appendCommitDurably(index);
                }
                @Override public void rewriteDurably(List<LogEntry> entries, long commit) throws IOException {
                    physical.rewriteDurably(entries, commit);
                }
                @Override public long lastDurableLogIndex() { return physical.lastDurableLogIndex(); }
                @Override public long durableCommitIndex() { return physical.durableCommitIndex(); }
                @Override public void close() throws IOException { physical.close(); }
            };
        };

        try (var node = new NodeServer(config, 0, (c, port, client, peer) -> {
                    listenerCreationAttempted.set(true);
                    throw new AssertionError("replay must complete before the listener is constructed");
                }, injection)) {
            IOException failure = assertThrows(IOException.class, node::start);
            assertTrue(replayFaultInjected.get(), "the committed replay fault must actually be injected");
            assertTrue(failure.getMessage().contains("replay failed closed"), failure.toString());
            assertInstanceOf(IllegalStateException.class, failure.getCause(),
                    "the failure must come from real KvStateMachine.apply during committed replay");

            assertEquals("PUT command must contain a value", failure.getCause().getMessage());

            // Entry 1 really was applied in memory before entry 2 failed, but no
            // intermediate application progress is published and no data gate opens.
            assertNotNull(node.readApplierForTest());
            assertEquals(1, node.readApplierForTest().appliedIndex());
            assertTrue(node.readApplierForTest().failed());
            assertArrayEquals(new byte[]{4, 2},
                    node.readApplierForTest().diagnosticGetForTest("valid").orElseThrow());
            assertEquals(0, node.statusSnapshot().appliedIndex());
            assertEquals(NodeLifecycleState.FAILED, node.statusSnapshot().state());
            assertFalse(node.statusSnapshot().ready());
            assertEquals(-1, node.boundPort());
            assertFalse(listenerCreationAttempted.get());
            assertNull(node.leaderCoordinatorForTest(), "write admission must never be constructed");
            assertNull(node.readCoordinatorForTest(), "read admission must never be constructed");

            // The real gRPC service admission policy must reject both operations.
            // No transport listener exists because replay fails before binding.
            ClientKvGrpcService service = new ClientKvGrpcService(config, node::statusSnapshot,
                    node.leaderCoordinatorForTest(), node.readCoordinatorForTest());
            Reply<GetResponse> get = new Reply<>();
            service.get(GetRequest.newBuilder().setKey("valid").build(), get);
            assertEquals(ResultCode.RESULT_CODE_NOT_READY, get.completedValue().getCode());
            Reply<MutationResponse> put = new Reply<>();
            service.put(PutRequest.newBuilder().setRequestId("after-failure").setKey("valid")
                    .setValue(ByteString.copyFrom(new byte[]{9})).build(), put);
            assertEquals(ResultCode.RESULT_CODE_NOT_READY, put.completedValue().getCode());
        }
        // The injected failure was strictly in-memory: no COMMIT or WAL data vanished.
        try (var reopened = FileWalStore.open(path)) {
            assertEquals(2, reopened.durableCommitIndex());
            assertEquals(2, reopened.lastDurableLogIndex());
            assertEquals(second, reopened.recoveryResult().entries().get(1).command());
        }
        try (var lease = WalDirectoryLease.acquire(config.walDirectory())) {
            assertTrue(lease.isHeld(), "failed replay must not strand the directory lease");
        }
    }
}
