package io.github.ahmadbitaarr.raftkv.rpc;

import io.github.ahmadbitaarr.raftkv.rpc.generated.replication.v1.*;
import java.util.Objects;

/** Validate only schema semantics; cannot attest that a future follower actually forced a WAL. */
public final class ReplicationContractValidator {
    private ReplicationContractValidator() { }

    public static void validate(LogEntry r) {
        Objects.requireNonNull(r, "replication entry");
        RpcLimits.checkMessageSize(r);
        if (r.getIndex() < 1) throw new IllegalArgumentException("entry index must be positive");
        RpcLimits.validText(r.getRequestId(), RpcLimits.MAX_REQUEST_ID_BYTES, "request_id", true);
        RpcLimits.validText(r.getKey(), RpcLimits.MAX_KEY_BYTES, "key", false);
        RpcLimits.checkBytes(r.getCommandFingerprint().size(), 1, RpcLimits.MAX_FINGERPRINT_BYTES, "fingerprint");
        ProtoMapper.toDomainOperation(r.getOperation());
        switch (r.getOperation()) {
            case OPERATION_TYPE_PUT -> {
                if (!r.hasValue()) throw new IllegalArgumentException("PUT value must be present");
                RpcLimits.checkBytes(r.getValue().size(), 0, RpcLimits.MAX_VALUE_BYTES, "value");
            }
            case OPERATION_TYPE_DELETE -> {
                if (r.hasValue()) throw new IllegalArgumentException("DELETE must not carry a value");
            }
            default -> throw new IllegalArgumentException("unknown operation");
        }
    }

    public static void validate(ReplicateEntryRequest r) {
        Objects.requireNonNull(r, "replication request"); RpcLimits.checkMessageSize(r);
        node(r.getLeaderId(), "leader_id");
        if (!r.hasEntry()) throw new IllegalArgumentException("replicated entry is required");
        validate(r.getEntry());
        nonnegative(r.getLeaderCommitIndex(), "leader_commit_index");
    }

    public static void validate(ReplicateEntryResponse r) {
        Objects.requireNonNull(r, "replication response"); RpcLimits.checkMessageSize(r);
        node(r.getFollowerId(), "follower_id"); message(r.getMessage());
        nonnegative(r.getLastPersistedIndex(), "last_persisted_index");
        nonnegative(r.getCommitIndex(), "commit_index");
        if (r.getCommitIndex() > r.getLastPersistedIndex())
            throw new IllegalArgumentException("commit_index exceeds durable last index");
        if (r.getAccepted()) {
            if (r.getRejectReason() != ReplicationRejectReason.REPLICATION_REJECT_REASON_UNSPECIFIED || r.hasExpectedNextIndex())
                throw new IllegalArgumentException("accepted ACK must not carry rejection metadata");
        } else {
            switch (r.getRejectReason()) {
                case REPLICATION_REJECT_REASON_INDEX_GAP, REPLICATION_REJECT_REASON_INDEX_CONFLICT -> {
                    if (!r.hasExpectedNextIndex() || r.getExpectedNextIndex() < 1)
                        throw new IllegalArgumentException("gap/conflict requires expected_next_index >0");
                }
                case REPLICATION_REJECT_REASON_NOT_READY -> {
                    if (r.hasExpectedNextIndex()) throw new IllegalArgumentException("NOT_READY has no index reconciliation hint");
                }
                default -> throw new IllegalArgumentException("rejected ACK requires known reason");
            }
        }
    }

    /** Identity/context is optional, but when known, an accepted ACK must cover its requested entry. */
    public static void validate(ReplicateEntryResponse response, ReplicateEntryRequest originatingRequest) {
        validate(originatingRequest);validate(response);
        if (response.getAccepted() && response.getLastPersistedIndex() < originatingRequest.getEntry().getIndex())
            throw new IllegalArgumentException("ACK does not cover requested entry index");
    }

    public static void validate(AdvanceCommitRequest r) {
        Objects.requireNonNull(r, "advance commit request"); RpcLimits.checkMessageSize(r);
        node(r.getLeaderId(), "leader_id");nonnegative(r.getCommitIndex(), "commit_index");
    }

    public static void validate(AdvanceCommitResponse r) {
        Objects.requireNonNull(r, "advance commit response"); RpcLimits.checkMessageSize(r);
        node(r.getFollowerId(), "follower_id");
        nonnegative(r.getCommitIndex(), "commit_index");nonnegative(r.getAppliedIndex(), "applied_index");
        if (r.getAppliedIndex() > r.getCommitIndex()) throw new IllegalArgumentException("applied_index exceeds commit_index");
    }

    public static void validate(ReplicationStatusRequest r) {
        Objects.requireNonNull(r, "status request"); RpcLimits.checkMessageSize(r);
        node(r.getLeaderId(), "leader_id");
    }

    public static void validate(ReplicationStatusResponse r) {
        Objects.requireNonNull(r, "status response"); RpcLimits.checkMessageSize(r);
        node(r.getNodeId(), "node_id");
        if (r.getRole() != NodeRole.NODE_ROLE_LEADER && r.getRole() != NodeRole.NODE_ROLE_FOLLOWER)
            throw new IllegalArgumentException("node role must be configured leader or follower");
        nonnegative(r.getLastLogIndex(), "last_log_index"); nonnegative(r.getCommitIndex(), "commit_index");
        nonnegative(r.getAppliedIndex(), "applied_index");
        if (r.getAppliedIndex() > r.getCommitIndex() || r.getCommitIndex() > r.getLastLogIndex())
            throw new IllegalArgumentException("invalid index ordering: applied<=commit<=log required");
    }

    public static void validate(TruncateSuffixRequest r) {
        Objects.requireNonNull(r, "truncate request"); RpcLimits.checkMessageSize(r);
        node(r.getLeaderId(), "leader_id"); nonnegative(r.getTruncateAfterIndex(), "truncate_after_index");
    }

    public static void validate(TruncateSuffixResponse r) {
        Objects.requireNonNull(r, "truncate response"); RpcLimits.checkMessageSize(r);
        node(r.getNodeId(), "node_id");
        nonnegative(r.getLastLogIndex(), "last_log_index"); nonnegative(r.getCommitIndex(), "commit_index");
        if (r.getCommitIndex() > r.getLastLogIndex()) throw new IllegalArgumentException("commit_index exceeds log");
    }

    private static void node(String value, String field) { RpcLimits.validText(value, RpcLimits.MAX_NODE_ID_BYTES, field, true); }
    private static void nonnegative(long index, String field) {
        if (index < 0) throw new IllegalArgumentException(field + " must be nonnegative");
    }
    private static void message(String message) {
        if (RpcLimits.strictUtf8Length(message,"message") > RpcLimits.MAX_MESSAGE_TEXT_BYTES)
            throw new IllegalArgumentException("message too long");
    }
}
