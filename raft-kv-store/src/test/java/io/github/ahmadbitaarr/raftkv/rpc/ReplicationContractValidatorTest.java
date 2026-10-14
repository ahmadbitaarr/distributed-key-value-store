package io.github.ahmadbitaarr.raftkv.rpc;

import com.google.protobuf.ByteString;
import io.github.ahmadbitaarr.raftkv.rpc.generated.replication.v1.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ReplicationContractValidatorTest {
    private static LogEntry entry() { return LogEntry.newBuilder().setIndex(1).setRequestId("id").setOperation(OperationType.OPERATION_TYPE_PUT)
            .setKey("k").setValue(ByteString.EMPTY).setCommandFingerprint(ByteString.copyFromUtf8("f")).build(); }
    private static ReplicateEntryRequest req() { return ReplicateEntryRequest.newBuilder().setLeaderId("leader").setEntry(entry()).build(); }
    private static ReplicateEntryResponse.Builder accepted() { return ReplicateEntryResponse.newBuilder().setFollowerId("f").setAccepted(true).setLastPersistedIndex(1).setCommitIndex(0); }
    @Test void replicateEntryRequestPreservesMetadata() { assertDoesNotThrow(() -> ReplicationContractValidator.validate(req())); }
    @Test void replicateEntryRejectsMissingEntry() { assertThrows(IllegalArgumentException.class, () -> ReplicationContractValidator.validate(req().toBuilder().clearEntry().build())); }
    @Test void replicateEntryRejectsNegativeLeaderCommitIndex() { assertThrows(IllegalArgumentException.class, () -> ReplicationContractValidator.validate(req().toBuilder().setLeaderCommitIndex(-1).build())); }
    @Test void durableAckContractAllowsOnlyValidAcceptedResponse() {
        assertDoesNotThrow(() -> ReplicationContractValidator.validate(accepted().build()));
        assertThrows(IllegalArgumentException.class, () -> ReplicationContractValidator.validate(accepted().setRejectReason(ReplicationRejectReason.REPLICATION_REJECT_REASON_INDEX_GAP).build()));
        assertThrows(IllegalArgumentException.class, () -> ReplicationContractValidator.validate(accepted().setExpectedNextIndex(2).build()));
    }
    @Test void rejectedGapIncludesExpectedNextIndex() {
        assertDoesNotThrow(() -> ReplicationContractValidator.validate(accepted().setAccepted(false).setRejectReason(ReplicationRejectReason.REPLICATION_REJECT_REASON_INDEX_GAP).setExpectedNextIndex(2).build()));
        assertThrows(IllegalArgumentException.class, () -> ReplicationContractValidator.validate(accepted().setAccepted(false).setRejectReason(ReplicationRejectReason.REPLICATION_REJECT_REASON_INDEX_GAP).build()));
    }
    @Test void rejectedConflictIncludesExpectedNextIndex() {
        assertDoesNotThrow(() -> ReplicationContractValidator.validate(accepted().setAccepted(false).setRejectReason(ReplicationRejectReason.REPLICATION_REJECT_REASON_INDEX_CONFLICT).setExpectedNextIndex(2).build()));
    }
    @Test void rejectsUnknownReplicateReason() {
        assertThrows(IllegalArgumentException.class, () -> ReplicationContractValidator.validate(accepted().setAccepted(false).setRejectReasonValue(99).build()));
    }
    @Test void rejectsNegativeOrInconsistentDurableIndexes() {
        assertThrows(IllegalArgumentException.class, () -> ReplicationContractValidator.validate(accepted().setCommitIndex(2).build()));
        assertThrows(IllegalArgumentException.class, () -> ReplicationContractValidator.validate(accepted().setLastPersistedIndex(-1).build()));
    }
    @Test void advanceCommitPreservesTarget() {
        var r = AdvanceCommitRequest.newBuilder().setLeaderId("leader").setCommitIndex(4).build();
        assertDoesNotThrow(() -> ReplicationContractValidator.validate(r)); assertEquals(4, r.getCommitIndex());
    }
    @Test void advanceCommitRejectsNegativeIndex() { assertThrows(IllegalArgumentException.class, () -> ReplicationContractValidator.validate(AdvanceCommitRequest.newBuilder().setLeaderId("l").setCommitIndex(-1).build())); }
    @Test void advanceCommitResponsePreservesAppliedIndex() {
        assertDoesNotThrow(() -> ReplicationContractValidator.validate(AdvanceCommitResponse.newBuilder().setFollowerId("f").setCommitIndex(3).setAppliedIndex(2).build()));
    }
    @Test void statusPreservesRoleReadinessAndIndexes() {
        var r=ReplicationStatusResponse.newBuilder().setNodeId("f").setRole(NodeRole.NODE_ROLE_FOLLOWER).setLastLogIndex(3).setCommitIndex(2).setAppliedIndex(1).setReady(true).build();
        assertDoesNotThrow(() -> ReplicationContractValidator.validate(r)); assertTrue(r.getReady());
    }
    @Test void statusRejectsUnknownRole() {
        assertThrows(IllegalArgumentException.class, () -> ReplicationContractValidator.validate(ReplicationStatusResponse.newBuilder().setNodeId("f").setRoleValue(200).build()));
    }
    @Test void statusRejectsAppliedGreaterThanCommit() {
        assertThrows(IllegalArgumentException.class, () -> ReplicationContractValidator.validate(ReplicationStatusResponse.newBuilder().setNodeId("f").setRole(NodeRole.NODE_ROLE_FOLLOWER).setAppliedIndex(2).setCommitIndex(1).setLastLogIndex(3).build()));
    }
    @Test void statusRejectsCommitGreaterThanLastLog() {
        assertThrows(IllegalArgumentException.class, () -> ReplicationContractValidator.validate(ReplicationStatusResponse.newBuilder().setNodeId("f").setRole(NodeRole.NODE_ROLE_FOLLOWER).setCommitIndex(3).setLastLogIndex(2).build()));
    }
    @Test void truncateSuffixAllowsZeroTarget() { assertDoesNotThrow(() -> ReplicationContractValidator.validate(TruncateSuffixRequest.newBuilder().setLeaderId("l").setTruncateAfterIndex(0).build())); }
    @Test void truncateSuffixRejectsNegativeTarget() { assertThrows(IllegalArgumentException.class, () -> ReplicationContractValidator.validate(TruncateSuffixRequest.newBuilder().setLeaderId("l").setTruncateAfterIndex(-1).build())); }
    @Test void truncateResponseRespectsCommitBoundaryMetadata() {
        assertThrows(IllegalArgumentException.class, () -> ReplicationContractValidator.validate(TruncateSuffixResponse.newBuilder().setNodeId("f").setCommitIndex(4).setLastLogIndex(2).build()));
    }
    @Test void statusRequestRejectsBlankLeader() { assertThrows(IllegalArgumentException.class, () -> ReplicationContractValidator.validate(ReplicationStatusRequest.getDefaultInstance())); }
    @Test void acceptedReplyRequiresDurableIndexCoveringRequestWhenContextAvailable() {
        var a=accepted().setLastPersistedIndex(0).build();
        assertThrows(IllegalArgumentException.class, () -> ReplicationContractValidator.validate(a,req()));
    }
}
