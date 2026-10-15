package io.github.ahmadbitaarr.raftkv.rpc;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.github.ahmadbitaarr.raftkv.config.ClusterConfigTest;
import io.github.ahmadbitaarr.raftkv.rpc.generated.replication.v1.*;
import io.github.ahmadbitaarr.raftkv.server.NodeLifecycleState;
import io.github.ahmadbitaarr.raftkv.server.NodeRole;
import io.github.ahmadbitaarr.raftkv.server.NodeStatusSnapshot;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import static org.junit.jupiter.api.Assertions.*;

class ReplicationGrpcTerminalSignalTest {
    @TempDir Path dir;

    private ReplicationGrpcService service() {
        var snapshot = new NodeStatusSnapshot("n2", NodeRole.FOLLOWER,
                NodeLifecycleState.CONTROL_ONLY, 0, 0, 0, false);
        return new ReplicationGrpcService(ClusterConfigTest.config(dir, "n2"), () -> snapshot);
    }

    @Test void statusOnNextFailureCannotEmitSecondTerminalSignalOrBecomeInvalidArgument() {
        var next = new AtomicInteger();
        var error = new AtomicInteger();
        var complete = new AtomicInteger();
        var thrown = new IllegalArgumentException("failure inside observer, not request");
        StreamObserver<ReplicationStatusResponse> observer = new StreamObserver<>() {
            @Override public void onNext(ReplicationStatusResponse value) {next.incrementAndGet(); throw thrown;}
            @Override public void onError(Throwable value) {error.incrementAndGet();}
            @Override public void onCompleted() {complete.incrementAndGet();}
        };
        assertSame(thrown, assertThrows(IllegalArgumentException.class, () -> service().getReplicationStatus(
                ReplicationStatusRequest.newBuilder().setLeaderId("n1").build(), observer)));
        assertEquals(1, next.get());
        assertEquals(0, error.get());
        assertEquals(0, complete.get());
    }

    @Test void statusOnCompletedFailureDoesNotSendOnError() {
        var next = new AtomicInteger();
        var error = new AtomicInteger();
        var complete = new AtomicInteger();
        var thrown = Status.CANCELLED.asRuntimeException();
        StreamObserver<ReplicationStatusResponse> observer = new StreamObserver<>() {
            @Override public void onNext(ReplicationStatusResponse value) {next.incrementAndGet();}
            @Override public void onError(Throwable value) {error.incrementAndGet();}
            @Override public void onCompleted() {complete.incrementAndGet(); throw thrown;}
        };
        assertSame(thrown, assertThrows(StatusRuntimeException.class, () -> service().getReplicationStatus(
                ReplicationStatusRequest.newBuilder().setLeaderId("n1").build(), observer)));
        assertEquals(1, next.get());
        assertEquals(0, error.get());
        assertEquals(1, complete.get());
    }

    @Test void statusOnErrorFailureDoesNotReenterObserver() {
        var errors = new AtomicInteger();
        var thrown = new IllegalArgumentException("observer rejected terminal signal");
        StreamObserver<ReplicationStatusResponse> observer = new StreamObserver<>() {
            @Override public void onNext(ReplicationStatusResponse value) {fail("unexpected onNext");}
            @Override public void onError(Throwable value) {
                errors.incrementAndGet();
                assertEquals(Status.Code.INVALID_ARGUMENT, Status.fromThrowable(value).getCode());
                throw thrown;
            }
            @Override public void onCompleted() {fail("unexpected completion");}
        };
        assertSame(thrown, assertThrows(IllegalArgumentException.class,
                () -> service().getReplicationStatus(ReplicationStatusRequest.getDefaultInstance(), observer)));
        assertEquals(1, errors.get());
    }

    @Test void advanceCommitOnErrorFailureDoesNotBecomeInvalidArgumentOrSendTwice() {
        var errors = new AtomicInteger();
        var thrown = new IllegalArgumentException("observer failed");
        StreamObserver<AdvanceCommitResponse> observer = new StreamObserver<>() {
            @Override public void onNext(AdvanceCommitResponse value) {fail("disabled operation must not succeed");}
            @Override public void onError(Throwable value) {
                errors.incrementAndGet();
                assertEquals(Status.Code.FAILED_PRECONDITION, Status.fromThrowable(value).getCode());
                throw thrown;
            }
            @Override public void onCompleted() {fail("disabled operation must not complete");}
        };
        assertSame(thrown, assertThrows(IllegalArgumentException.class,
                () -> service().advanceCommit(AdvanceCommitRequest.newBuilder().setLeaderId("n1").build(), observer)));
        assertEquals(1, errors.get());
    }

    @Test void truncateOnErrorFailureDoesNotBecomeInvalidArgumentOrSendTwice() {
        var errors = new AtomicInteger();
        var thrown = Status.CANCELLED.asRuntimeException();
        StreamObserver<TruncateSuffixResponse> observer = new StreamObserver<>() {
            @Override public void onNext(TruncateSuffixResponse value) {fail("disabled operation must not succeed");}
            @Override public void onError(Throwable value) {
                errors.incrementAndGet();
                assertEquals(Status.Code.FAILED_PRECONDITION, Status.fromThrowable(value).getCode());
                throw thrown;
            }
            @Override public void onCompleted() {fail("disabled operation must not complete");}
        };
        assertSame(thrown, assertThrows(StatusRuntimeException.class,
                () -> service().truncateUncommittedSuffix(TruncateSuffixRequest.newBuilder().setLeaderId("n1").build(), observer)));
        assertEquals(1, errors.get());
    }

    @Test void invalidRequestsStillMapToInvalidArgumentWithoutCallbackReentry() {
        var status = new AtomicReference<Throwable>();
        var advance = new AtomicReference<Throwable>();
        var truncate = new AtomicReference<Throwable>();
        service().getReplicationStatus(ReplicationStatusRequest.getDefaultInstance(), errorOnly(status));
        service().advanceCommit(AdvanceCommitRequest.getDefaultInstance(), errorOnly(advance));
        service().truncateUncommittedSuffix(TruncateSuffixRequest.getDefaultInstance(), errorOnly(truncate));
        assertEquals(Status.Code.INVALID_ARGUMENT, Status.fromThrowable(status.get()).getCode());
        assertEquals(Status.Code.INVALID_ARGUMENT, Status.fromThrowable(advance.get()).getCode());
        assertEquals(Status.Code.INVALID_ARGUMENT, Status.fromThrowable(truncate.get()).getCode());
    }

    private static <T> StreamObserver<T> errorOnly(AtomicReference<Throwable> captured) {
        return new StreamObserver<>() {
            @Override public void onNext(T value) {fail("unexpected success");}
            @Override public void onError(Throwable failure) {assertNull(captured.getAndSet(failure), "only one terminal signal");}
            @Override public void onCompleted() {fail("unexpected completion");}
        };
    }
}
