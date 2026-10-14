package io.github.ahmadbitaarr.raftkv.rpc;

import com.google.protobuf.ByteString;
import io.github.ahmadbitaarr.raftkv.log.Command;
import io.github.ahmadbitaarr.raftkv.log.LogEntry;
import io.github.ahmadbitaarr.raftkv.rpc.generated.replication.v1.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ProtoMapperTest {
    private LogEntry put(byte[] value) { return new LogEntry(1,"req",Command.put("key",value),new byte[]{0, (byte)0xff}); }
    @Test void putLogEntryRoundTrips() { assertEquals(put(new byte[]{1,2}), ProtoMapper.toDomainLogEntry(ProtoMapper.toProtoLogEntry(put(new byte[]{1,2})))); }
    @Test void deleteLogEntryRoundTrips() {
        var entry = new LogEntry(3,"req",Command.delete("key"), new byte[]{9});
        var proto = ProtoMapper.toProtoLogEntry(entry);
        assertFalse(proto.hasValue());
        assertEquals(entry, ProtoMapper.toDomainLogEntry(proto));
    }
    @Test void emptyPutValueRemainsPresentOnRoundTrip() {
        var proto = ProtoMapper.toProtoLogEntry(put(new byte[0]));
        assertTrue(proto.hasValue()); assertTrue(ProtoMapper.toDomainLogEntry(proto).command().value().orElseThrow().length == 0);
    }
    @Test void arbitraryBinaryValueRoundTrips() {
        byte[] v = {0, (byte)128, (byte)255, 0, 5};
        assertArrayEquals(v, ProtoMapper.toDomainLogEntry(ProtoMapper.toProtoLogEntry(put(v))).command().value().orElseThrow());
    }
    @Test void binaryFingerprintRoundTrips() {
        assertArrayEquals(new byte[]{0,(byte)255}, ProtoMapper.toDomainLogEntry(ProtoMapper.toProtoLogEntry(put(new byte[]{1}))).commandFingerprint());
    }
    @Test void unicodeKeyAndRequestIdRoundTrip() {
        var entry = new LogEntry(4,"réquête 🚀",Command.put("钥匙🔑",new byte[]{3}),new byte[]{3});
        assertEquals(entry, ProtoMapper.toDomainLogEntry(ProtoMapper.toProtoLogEntry(entry)));
    }
    @Test void domainInputArrayMutationCannotChangeProtobuf() {
        byte[] v = {1,2}; var e=put(v); var proto=ProtoMapper.toProtoLogEntry(e); v[0]=55;
        assertArrayEquals(new byte[]{1,2}, proto.getValue().toByteArray());
    }
    @Test void protobufMappingCannotMutateDomainCommand() {
        var domain=ProtoMapper.toDomainLogEntry(ProtoMapper.toProtoLogEntry(put(new byte[]{2}))); var bytes=domain.command().value().orElseThrow();
        bytes[0]=42; assertArrayEquals(new byte[]{2}, domain.command().value().orElseThrow());
    }
    private io.github.ahmadbitaarr.raftkv.rpc.generated.replication.v1.LogEntry.Builder valid() {
        return ProtoMapper.toProtoLogEntry(put(new byte[]{1})).toBuilder();
    }
    @Test void rejectsNonPositiveLogIndex() {
        assertThrows(IllegalArgumentException.class, () -> ProtoMapper.toDomainLogEntry(valid().setIndex(0).build()));
        assertThrows(IllegalArgumentException.class, () -> ProtoMapper.toDomainLogEntry(valid().setIndex(-1).build()));
    }
    @Test void rejectsMissingNestedEntryWhereRequired() {
        assertThrows(IllegalArgumentException.class, () -> ReplicationContractValidator.validate(ReplicateEntryRequest.newBuilder().setLeaderId("n").build()));
    }
    @Test void rejectsUnspecifiedOrUnknownOperation() {
        assertThrows(IllegalArgumentException.class, () -> ProtoMapper.toDomainLogEntry(valid().setOperation(OperationType.OPERATION_TYPE_UNSPECIFIED).build()));
        assertThrows(IllegalArgumentException.class, () -> ProtoMapper.toDomainLogEntry(valid().setOperationValue(99).build()));
    }
    @Test void rejectsPutWithoutValuePresence() {
        assertThrows(IllegalArgumentException.class, () -> ProtoMapper.toDomainLogEntry(valid().clearValue().build()));
    }
    @Test void rejectsDeleteWithValuePresence() {
        assertThrows(IllegalArgumentException.class, () -> ProtoMapper.toDomainLogEntry(valid().setOperation(OperationType.OPERATION_TYPE_DELETE).build()));
    }
    @Test void rejectsEmptyFingerprint() {
        assertThrows(IllegalArgumentException.class, () -> ProtoMapper.toDomainLogEntry(valid().clearCommandFingerprint().build()));
    }
    @Test void rejectsOversizedEntryFields() {
        assertThrows(IllegalArgumentException.class, () -> ProtoMapper.toDomainLogEntry(valid().setKey("a".repeat(RpcLimits.MAX_KEY_BYTES+1)).build()));
    }
    @Test void rejectsIllFormedJavaUtf8Strings() {
        assertThrows(IllegalArgumentException.class, () -> ProtoMapper.toProtoLogEntry(new LogEntry(1,"req",Command.put("\uD800",new byte[0]),new byte[]{1})));
    }
}
