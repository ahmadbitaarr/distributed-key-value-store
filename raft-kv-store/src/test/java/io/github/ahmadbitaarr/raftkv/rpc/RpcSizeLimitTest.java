package io.github.ahmadbitaarr.raftkv.rpc;

import com.google.protobuf.ByteString;
import io.github.ahmadbitaarr.raftkv.rpc.generated.client.v1.*;
import io.github.ahmadbitaarr.raftkv.rpc.generated.replication.v1.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class RpcSizeLimitTest {
    @Test void maxWalSupportedPutValuePassesSemanticValidation() {
        var req=PutRequest.newBuilder().setRequestId("id").setKey("k").setValue(ByteString.copyFrom(new byte[RpcLimits.MAX_VALUE_BYTES])).build();
        assertDoesNotThrow(() -> ClientContractValidator.validate(req));
    }
    @Test void oneByteOverMaxPutValueIsRejected() {
        var req=PutRequest.newBuilder().setRequestId("id").setKey("k").setValue(ByteString.copyFrom(new byte[RpcLimits.MAX_VALUE_BYTES+1])).build();
        assertThrows(IllegalArgumentException.class, () -> ClientContractValidator.validate(req));
    }
    @Test void maxSizedPutRequestFitsGrpcBudget() {
        var req=PutRequest.newBuilder().setRequestId("r".repeat(1024)).setKey("k".repeat(65536)).setValue(ByteString.copyFrom(new byte[RpcLimits.MAX_VALUE_BYTES])).build();
        assertDoesNotThrow(() -> ClientContractValidator.validate(req));
    }
    @Test void maxSizedGetResponseFitsGrpcBudget() {
        var res=GetResponse.newBuilder().setCode(ResultCode.RESULT_CODE_OK).setKey("k".repeat(65536)).setValue(ByteString.copyFrom(new byte[RpcLimits.MAX_VALUE_BYTES])).setCommitIndex(1).build();
        assertDoesNotThrow(() -> ClientContractValidator.validate(res));
    }
    @Test void maxSizedReplicateEntryFitsGrpcBudget() {
        var entry=io.github.ahmadbitaarr.raftkv.rpc.generated.replication.v1.LogEntry.newBuilder().setIndex(1)
                .setRequestId("r".repeat(1024)).setKey("k".repeat(65536)).setValue(ByteString.copyFrom(new byte[RpcLimits.MAX_VALUE_BYTES]))
                .setOperation(OperationType.OPERATION_TYPE_PUT).setCommandFingerprint(ByteString.copyFrom(new byte[1024])).build();
        var req=ReplicateEntryRequest.newBuilder().setLeaderId("l".repeat(256)).setEntry(entry).build();
        assertDoesNotThrow(() -> ReplicationContractValidator.validate(req));
    }
    @Test void overBudgetSerializedMessageIsRejected() {
        var oversized=GetResponse.newBuilder().setCode(ResultCode.RESULT_CODE_OK).setKey("k").setCommitIndex(1)
                .setValue(ByteString.copyFrom(new byte[RpcLimits.MAX_SERIALIZED_MESSAGE_BYTES+1])).build();
        assertThrows(IllegalArgumentException.class, () -> RpcLimits.checkMessageSize(oversized));
    }
    @Test void utf8LengthNotCharacterCountDeterminesLimit() {
        assertThrows(IllegalArgumentException.class, () -> RpcLimits.validText("é".repeat(513),RpcLimits.MAX_REQUEST_ID_BYTES,"id",true));
    }
    @Test void transportInboundLimitConstantsAreSymmetric() {
        assertEquals(12*1024*1024,RpcLimits.MAX_SERIALIZED_MESSAGE_BYTES);
        assertEquals(RpcLimits.MAX_SERIALIZED_MESSAGE_BYTES,RpcLimits.CLIENT_INBOUND_MESSAGE_BYTES);
        assertEquals(RpcLimits.MAX_SERIALIZED_MESSAGE_BYTES,RpcLimits.SERVER_INBOUND_MESSAGE_BYTES);
    }
}
