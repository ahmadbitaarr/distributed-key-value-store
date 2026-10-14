package io.github.ahmadbitaarr.raftkv.rpc;

import com.google.protobuf.ByteString;
import io.grpc.*;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.github.ahmadbitaarr.raftkv.rpc.generated.client.v1.*;
import io.github.ahmadbitaarr.raftkv.rpc.generated.replication.v1.*;
import org.junit.jupiter.api.*;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;

class GrpcUnaryContractTest {
    private Server server;
    private ManagedChannel channel;
    @BeforeEach void setup() throws IOException {
        String name=InProcessServerBuilder.generateName();
        server=InProcessServerBuilder.forName(name).directExecutor().addService(new FakeClient())
                .addService(new FakeReplication()).build().start();
        channel=InProcessChannelBuilder.forName(name).directExecutor().build();
    }
    @AfterEach void teardown() { channel.shutdownNow();server.shutdownNow(); }
    private KeyValueServiceGrpc.KeyValueServiceBlockingStub client() { return KeyValueServiceGrpc.newBlockingStub(channel).withDeadlineAfter(2,TimeUnit.SECONDS); }
    private ReplicationServiceGrpc.ReplicationServiceBlockingStub peer() { return ReplicationServiceGrpc.newBlockingStub(channel).withDeadlineAfter(2,TimeUnit.SECONDS); }
    @Test void inProcessPutUnaryStubDispatches() {
        var res=client().put(PutRequest.newBuilder().setRequestId("r").setKey("k").setValue(ByteString.EMPTY).build());
        assertEquals("r",res.getRequestId()); ClientContractValidator.validate(res);
    }
    @Test void inProcessGetUnaryStubDispatches() {
        var res=client().get(GetRequest.newBuilder().setKey("k").build());
        assertEquals(ResultCode.RESULT_CODE_NOT_FOUND,res.getCode()); ClientContractValidator.validate(res);
    }
    @Test void inProcessDeleteUnaryStubDispatches() { assertEquals(ResultCode.RESULT_CODE_OK,client().delete(DeleteRequest.newBuilder().setRequestId("r").setKey("k").build()).getCode()); }
    @Test void inProcessReplicateEntryUnaryStubDispatches() {
        var req=ReplicateEntryRequest.newBuilder().setLeaderId("l").setEntry(io.github.ahmadbitaarr.raftkv.rpc.generated.replication.v1.LogEntry.newBuilder()
                .setIndex(1).setRequestId("r").setKey("k").setOperation(OperationType.OPERATION_TYPE_PUT).setValue(ByteString.EMPTY)
                .setCommandFingerprint(ByteString.copyFromUtf8("f"))).build();
        var res=peer().replicateEntry(req);
        assertFalse(res.getAccepted()); ReplicationContractValidator.validate(res);
    }
    @Test void inProcessAdvanceCommitUnaryStubDispatches() {
        assertEquals(0,peer().advanceCommit(AdvanceCommitRequest.newBuilder().setLeaderId("l").build()).getCommitIndex());
    }
    @Test void inProcessStatusUnaryStubDispatches() {
        var res=peer().getReplicationStatus(ReplicationStatusRequest.newBuilder().setLeaderId("l").build());
        assertFalse(res.getReady()); ReplicationContractValidator.validate(res);
    }
    @Test void inProcessTruncateUnaryStubDispatches() {
        assertEquals(0,peer().truncateUncommittedSuffix(TruncateSuffixRequest.newBuilder().setLeaderId("l").build()).getLastLogIndex());
    }
    @Test void grpcInvalidArgumentStatusIsObservable() {
        var ex=assertThrows(StatusRuntimeException.class, () -> client().put(PutRequest.getDefaultInstance()));
        assertEquals(Status.Code.INVALID_ARGUMENT,ex.getStatus().getCode());
    }
    @Test void grpcFailedPreconditionStatusIsObservable() {
        var ex=assertThrows(StatusRuntimeException.class, () -> peer().advanceCommit(AdvanceCommitRequest.newBuilder().setLeaderId("wrong").build()));
        assertEquals(Status.Code.FAILED_PRECONDITION,ex.getStatus().getCode());
    }
    @Test void grpcDeadlineExceededIsObservable() {
        var ex=assertThrows(StatusRuntimeException.class, () -> KeyValueServiceGrpc.newBlockingStub(channel)
                .withDeadlineAfter(75,TimeUnit.MILLISECONDS).get(GetRequest.newBuilder().setKey("hang").build()));
        assertEquals(Status.Code.DEADLINE_EXCEEDED,ex.getStatus().getCode());
    }
    @Test void applicationNotLeaderIsDistinctFromGrpcError() {
        var res=client().get(GetRequest.newBuilder().setKey("redirect").build());
        assertEquals(ResultCode.RESULT_CODE_NOT_LEADER,res.getCode());
        ClientContractValidator.validate(res);
    }
    private static final class FakeClient extends KeyValueServiceGrpc.KeyValueServiceImplBase {
        @Override public void put(PutRequest r,StreamObserver<MutationResponse> o) {
            if (!r.hasValue() || r.getRequestId().isBlank() || r.getKey().isEmpty()) { o.onError(Status.INVALID_ARGUMENT.asRuntimeException());return; }
            o.onNext(MutationResponse.newBuilder().setCode(ResultCode.RESULT_CODE_OK).setRequestId(r.getRequestId()).setCommitIndex(1).build());o.onCompleted();
        }
        @Override public void delete(DeleteRequest r,StreamObserver<MutationResponse> o) {
            o.onNext(MutationResponse.newBuilder().setCode(ResultCode.RESULT_CODE_OK).setRequestId(r.getRequestId()).setCommitIndex(1).build());o.onCompleted();
        }
        @Override public void get(GetRequest r,StreamObserver<GetResponse> o) {
            if (r.getKey().equals("hang")) return;
            if (r.getKey().equals("redirect")) {
                o.onNext(GetResponse.newBuilder().setKey(r.getKey()).setCode(ResultCode.RESULT_CODE_NOT_LEADER)
                        .setLeaderHint(LeaderHint.newBuilder().setLeaderId("leader").setHost("localhost").setPort(7777)).build());
            } else {o.onNext(GetResponse.newBuilder().setCode(ResultCode.RESULT_CODE_NOT_FOUND).setKey(r.getKey()).setCommitIndex(0).build());}
            o.onCompleted();
        }
    }
    private static final class FakeReplication extends ReplicationServiceGrpc.ReplicationServiceImplBase {
        @Override public void replicateEntry(ReplicateEntryRequest r,StreamObserver<ReplicateEntryResponse> o) {
            o.onNext(ReplicateEntryResponse.newBuilder().setFollowerId("f").setRejectReason(ReplicationRejectReason.REPLICATION_REJECT_REASON_NOT_READY).build());o.onCompleted();
        }
        @Override public void advanceCommit(AdvanceCommitRequest r,StreamObserver<AdvanceCommitResponse> o) {
            if (r.getLeaderId().equals("wrong")) { o.onError(Status.FAILED_PRECONDITION.asRuntimeException());return; }
            o.onNext(AdvanceCommitResponse.newBuilder().setFollowerId("f").build());o.onCompleted();
        }
        @Override public void getReplicationStatus(ReplicationStatusRequest r,StreamObserver<ReplicationStatusResponse> o) {
            o.onNext(ReplicationStatusResponse.newBuilder().setNodeId("f").setRole(NodeRole.NODE_ROLE_FOLLOWER).build());o.onCompleted();
        }
        @Override public void truncateUncommittedSuffix(TruncateSuffixRequest r,StreamObserver<TruncateSuffixResponse> o) {
            o.onNext(TruncateSuffixResponse.newBuilder().setNodeId("f").build());o.onCompleted();
        }
    }
}
