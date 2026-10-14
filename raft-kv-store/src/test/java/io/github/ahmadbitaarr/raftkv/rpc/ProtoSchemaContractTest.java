package io.github.ahmadbitaarr.raftkv.rpc;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import io.github.ahmadbitaarr.raftkv.rpc.generated.client.v1.*;
import io.github.ahmadbitaarr.raftkv.rpc.generated.replication.v1.ReplicationApiProto;
import io.github.ahmadbitaarr.raftkv.rpc.generated.replication.v1.ReplicationServiceGrpc;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ProtoSchemaContractTest {
    @Test void generatedClientTypesAndServiceExist() {
        assertNotNull(PutRequest.getDefaultInstance());
        assertEquals("raftkv.client.v1.KeyValueService", KeyValueServiceGrpc.SERVICE_NAME);
    }
    @Test void generatedReplicationTypesAndServiceExist() {
        assertNotNull(io.github.ahmadbitaarr.raftkv.rpc.generated.replication.v1.LogEntry.getDefaultInstance());
        assertEquals("raftkv.replication.v1.ReplicationService", ReplicationServiceGrpc.SERVICE_NAME);
    }
    @Test void clientServiceHasExactlyThreeUnaryMethods() {
        var methods = ClientApiProto.getDescriptor().findServiceByName("KeyValueService").getMethods();
        assertEquals(3, methods.size());
        assertEquals(java.util.List.of("Put", "Get", "Delete"), methods.stream().map(Descriptors.MethodDescriptor::getName).toList());
    }
    @Test void replicationServiceHasExactlyFourUnaryMethods() {
        var methods = ReplicationApiProto.getDescriptor().findServiceByName("ReplicationService").getMethods();
        assertEquals(java.util.List.of("ReplicateEntry", "AdvanceCommit", "GetReplicationStatus", "TruncateUncommittedSuffix"),
                methods.stream().map(Descriptors.MethodDescriptor::getName).toList());
    }
    @Test void clientFieldsRetainApprovedNumbersAndTypes() {
        var put = PutRequest.getDescriptor();
        assertEquals(3, put.findFieldByName("value").getNumber());
        assertTrue(put.findFieldByName("value").hasPresence());
        assertEquals(3, MutationResponse.getDescriptor().findFieldByName("commit_index").getNumber());
        assertTrue(MutationResponse.getDescriptor().findFieldByName("commit_index").hasPresence());
    }
    @Test void replicationFieldsRetainApprovedNumbersAndTypes() {
        var d = io.github.ahmadbitaarr.raftkv.rpc.generated.replication.v1.LogEntry.getDescriptor();
        assertEquals(6, d.findFieldByName("command_fingerprint").getNumber());
        assertTrue(d.findFieldByName("value").hasPresence());
        assertTrue(io.github.ahmadbitaarr.raftkv.rpc.generated.replication.v1.ReplicateEntryResponse.getDescriptor()
                .findFieldByName("expected_next_index").hasPresence());
    }
    @Test void allEnumsHaveUnspecifiedZero() {
        for (var e : ClientApiProto.getDescriptor().getEnumTypes()) assertEquals(0, e.getValues().getFirst().getNumber());
        for (var e : ReplicationApiProto.getDescriptor().getEnumTypes()) assertEquals(0, e.getValues().getFirst().getNumber());
    }
    @Test void protoDefaultEnumsAreNotValidDomainValues() {
        assertThrows(IllegalArgumentException.class, () -> ClientContractValidator.validate(
                MutationResponse.newBuilder().setRequestId("r").build()));
        assertThrows(IllegalArgumentException.class, () -> ProtoMapper.toDomainLogEntry(
                io.github.ahmadbitaarr.raftkv.rpc.generated.replication.v1.LogEntry.newBuilder()
                .setIndex(1).setRequestId("r").setKey("k").setValue(ByteString.EMPTY)
                .setCommandFingerprint(ByteString.copyFromUtf8("f")).build()));
    }
    @Test void putValuePresenceDistinguishesAbsentAndExplicitEmpty() {
        assertFalse(PutRequest.newBuilder().build().hasValue());
        assertTrue(PutRequest.newBuilder().setValue(ByteString.EMPTY).build().hasValue());
    }
    @Test void getResponseValuePresenceDistinguishesAbsentAndExplicitEmpty() {
        assertFalse(GetResponse.getDefaultInstance().hasValue());
        assertTrue(GetResponse.newBuilder().setValue(ByteString.EMPTY).build().hasValue());
    }
    @Test void optionalCommitIndexDistinguishesAbsentFromZero() {
        assertFalse(GetResponse.getDefaultInstance().hasCommitIndex());
        assertTrue(GetResponse.newBuilder().setCommitIndex(0).build().hasCommitIndex());
    }
    @Test void protobufWireSerializationRoundTrips() throws Exception {
        var req = PutRequest.newBuilder().setRequestId("r").setKey("k").setValue(ByteString.EMPTY).build();
        assertEquals(req, PutRequest.parseFrom(req.toByteArray()));
        var status = io.github.ahmadbitaarr.raftkv.rpc.generated.replication.v1.ReplicationStatusResponse
                .newBuilder().setNodeId("n").setRole(io.github.ahmadbitaarr.raftkv.rpc.generated.replication.v1.NodeRole.NODE_ROLE_FOLLOWER).build();
        assertEquals(status, io.github.ahmadbitaarr.raftkv.rpc.generated.replication.v1.ReplicationStatusResponse.parseFrom(status.toByteArray()));
    }
    @Test void unknownEnumNumericValueIsNotSilentlyMapped() {
        var b = io.github.ahmadbitaarr.raftkv.rpc.generated.replication.v1.LogEntry.newBuilder()
                .setIndex(1).setRequestId("r").setKey("k").setValue(ByteString.EMPTY)
                .setCommandFingerprint(ByteString.copyFromUtf8("f")).setOperationValue(999);
        assertThrows(IllegalArgumentException.class, () -> ProtoMapper.toDomainLogEntry(b.build()));
    }
}
