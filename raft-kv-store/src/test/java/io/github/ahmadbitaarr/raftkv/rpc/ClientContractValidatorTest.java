package io.github.ahmadbitaarr.raftkv.rpc;

import com.google.protobuf.ByteString;
import io.github.ahmadbitaarr.raftkv.rpc.generated.client.v1.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ClientContractValidatorTest {
    private static PutRequest.Builder put() { return PutRequest.newBuilder().setKey("k").setRequestId("id").setValue(ByteString.EMPTY); }
    private static MutationResponse.Builder ok() { return MutationResponse.newBuilder().setCode(ResultCode.RESULT_CODE_OK).setRequestId("id").setCommitIndex(1); }
    private static GetResponse.Builder getOk() { return GetResponse.newBuilder().setCode(ResultCode.RESULT_CODE_OK).setKey("k").setValue(ByteString.EMPTY).setCommitIndex(0); }
    private static LeaderHint hint() { return LeaderHint.newBuilder().setLeaderId("one").setHost("127.0.0.1").setPort(8080).build(); }
    @Test void rejectsMissingOrBlankPutRequestId() {
        assertThrows(IllegalArgumentException.class, () -> ClientContractValidator.validate(put().clearRequestId().build()));
        assertThrows(IllegalArgumentException.class, () -> ClientContractValidator.validate(put().setRequestId("  ").build()));
    }
    @Test void rejectsMissingOrBlankDeleteRequestId() {
        assertThrows(IllegalArgumentException.class, () -> ClientContractValidator.validate(DeleteRequest.newBuilder().setKey("k").build()));
    }
    @Test void rejectsEmptyKeyForAllClientRequests() {
        assertThrows(IllegalArgumentException.class, () -> ClientContractValidator.validate(put().clearKey().build()));
        assertThrows(IllegalArgumentException.class, () -> ClientContractValidator.validate(GetRequest.getDefaultInstance()));
        assertThrows(IllegalArgumentException.class, () -> ClientContractValidator.validate(DeleteRequest.newBuilder().setRequestId("id").build()));
    }
    @Test void rejectsOversizedUtf8KeyAndRequestId() {
        assertThrows(IllegalArgumentException.class, () -> ClientContractValidator.validate(put().setKey("x".repeat(RpcLimits.MAX_KEY_BYTES + 1)).build()));
        assertThrows(IllegalArgumentException.class, () -> ClientContractValidator.validate(put().setRequestId("é".repeat(513)).build()));
    }
    @Test void omittedPutValueIsRejected() { assertThrows(IllegalArgumentException.class, () -> ClientContractValidator.validate(put().clearValue().build())); }
    @Test void explicitEmptyPutValueIsAccepted() { assertDoesNotThrow(() -> ClientContractValidator.validate(put().build())); }
    @Test void validatesMutationOkHasPositiveCommitIndex() {
        assertDoesNotThrow(() -> ClientContractValidator.validate(ok().build()));
        assertThrows(IllegalArgumentException.class, () -> ClientContractValidator.validate(ok().setCommitIndex(0).build()));
        assertThrows(IllegalArgumentException.class, () -> ClientContractValidator.validate(ok().clearCommitIndex().build()));
    }
    @Test void validatesMutationNonOkHasNoCommitIndex() {
        assertThrows(IllegalArgumentException.class, () -> ClientContractValidator.validate(ok().setCode(ResultCode.RESULT_CODE_NO_QUORUM).build()));
        assertDoesNotThrow(() -> ClientContractValidator.validate(ok().setCode(ResultCode.RESULT_CODE_NO_QUORUM).clearCommitIndex().build()));
    }
    @Test void notLeaderRequiresWellFormedHint() {
        var response=ok().setCode(ResultCode.RESULT_CODE_NOT_LEADER).clearCommitIndex();
        assertThrows(IllegalArgumentException.class, () -> ClientContractValidator.validate(response.build()));
        assertDoesNotThrow(() -> ClientContractValidator.validate(response.setLeaderHint(hint()).build()));
    }
    @Test void nonNotLeaderDisallowsLeaderHint() {
        assertThrows(IllegalArgumentException.class, () -> ClientContractValidator.validate(ok().setLeaderHint(hint()).build()));
    }
    @Test void getOkRequiresPresentValueEvenIfEmpty() {
        assertDoesNotThrow(() -> ClientContractValidator.validate(getOk().build()));
        assertThrows(IllegalArgumentException.class, () -> ClientContractValidator.validate(getOk().clearValue().build()));
    }
    @Test void getNotFoundRequiresAbsentValueAndPresentReadIndex() {
        var response=getOk().setCode(ResultCode.RESULT_CODE_NOT_FOUND).clearValue();
        assertDoesNotThrow(() -> ClientContractValidator.validate(response.build()));
        assertThrows(IllegalArgumentException.class, () -> ClientContractValidator.validate(response.clearCommitIndex().build()));
        assertThrows(IllegalArgumentException.class, () -> ClientContractValidator.validate(response.setCommitIndex(0).setValue(ByteString.EMPTY).build()));
    }
    @Test void getReadIndexZeroCanBeExplicitlyPresent() { assertDoesNotThrow(() -> ClientContractValidator.validate(getOk().build())); }
    @Test void followerNotLeaderCannotMasqueradeAsGetNotFound() {
        assertThrows(IllegalArgumentException.class, () -> ClientContractValidator.validate(getOk().setCode(ResultCode.RESULT_CODE_NOT_LEADER).clearValue().clearCommitIndex().build()));
    }
    @Test void rejectsUnspecifiedOrUnknownResultCode() {
        assertThrows(IllegalArgumentException.class, () -> ClientContractValidator.validate(ok().setCode(ResultCode.RESULT_CODE_UNSPECIFIED).build()));
        assertThrows(IllegalArgumentException.class, () -> ClientContractValidator.validate(ok().setCodeValue(101).build()));
    }
    @Test void rejectsInvalidResultCodeForOperation() {
        assertThrows(IllegalArgumentException.class, () -> ClientContractValidator.validate(getOk().setCode(ResultCode.RESULT_CODE_REQUEST_ID_CONFLICT).build()));
        assertThrows(IllegalArgumentException.class, () -> ClientContractValidator.validate(ok().setCode(ResultCode.RESULT_CODE_NOT_FOUND).build()));
    }
    @Test void applicationNotFoundIsNotTransportStatus() {
        assertDoesNotThrow(() -> ClientContractValidator.validate(getOk().setCode(ResultCode.RESULT_CODE_NOT_FOUND).clearValue().build()));
        assertNotEquals(io.grpc.Status.Code.NOT_FOUND.name(), ResultCode.RESULT_CODE_NOT_FOUND.name());
    }
    @Test void rejectsBlankMutationResponseRequestId() {
        assertThrows(IllegalArgumentException.class, () -> ClientContractValidator.validate(ok().clearRequestId().build()));
        assertThrows(IllegalArgumentException.class, () -> ClientContractValidator.validate(ok().setRequestId("  ").build()));
    }
    @Test void rejectsMismatchedMutationResponseRequestId() {
        assertThrows(IllegalArgumentException.class, () -> ClientContractValidator.validate(ok().setRequestId("other").build(), put().build()));
        assertDoesNotThrow(() -> ClientContractValidator.validate(ok().build(), put().build()));
        assertThrows(IllegalArgumentException.class, () -> ClientContractValidator.validate(ok().build(), DeleteRequest.newBuilder().setRequestId("other").setKey("k").build()));
    }
    @Test void rejectsMismatchedGetResponseKey() {
        assertThrows(IllegalArgumentException.class, () -> ClientContractValidator.validate(getOk().setKey("other").build(), GetRequest.newBuilder().setKey("k").build()));
        assertDoesNotThrow(() -> ClientContractValidator.validate(getOk().build(), GetRequest.newBuilder().setKey("k").build()));
    }
    @Test void getDoesNotRequireNewQuorum() {
        assertThrows(IllegalArgumentException.class, () -> ClientContractValidator.validate(getOk().setCode(ResultCode.RESULT_CODE_NO_QUORUM).clearValue().clearCommitIndex().build()));
        assertDoesNotThrow(() -> ClientContractValidator.validate(getOk().setCode(ResultCode.RESULT_CODE_NOT_READY).clearValue().clearCommitIndex().build()));
        assertDoesNotThrow(() -> ClientContractValidator.validate(ok().setCode(ResultCode.RESULT_CODE_NO_QUORUM).clearCommitIndex().build()));
    }
}
