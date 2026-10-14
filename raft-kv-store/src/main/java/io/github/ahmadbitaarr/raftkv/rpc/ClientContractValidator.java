package io.github.ahmadbitaarr.raftkv.rpc;

import io.github.ahmadbitaarr.raftkv.rpc.generated.client.v1.*;
import java.util.Objects;

/** Semantic validation only. No RPC execution, leader routing, commit or retry policy. */
public final class ClientContractValidator {
    private ClientContractValidator() { }

    public static void validate(PutRequest r) {
        Objects.requireNonNull(r, "put request");
        RpcLimits.checkMessageSize(r);
        requestId(r.getRequestId()); key(r.getKey());
        if (!r.hasValue()) throw new IllegalArgumentException("PUT requires explicit value presence");
        RpcLimits.checkBytes(r.getValue().size(), 0, RpcLimits.MAX_VALUE_BYTES, "PUT value");
    }

    public static void validate(DeleteRequest r) {
        Objects.requireNonNull(r, "delete request");
        RpcLimits.checkMessageSize(r);
        requestId(r.getRequestId()); key(r.getKey());
    }

    public static void validate(GetRequest r) {
        Objects.requireNonNull(r, "get request");
        RpcLimits.checkMessageSize(r); key(r.getKey());
    }

    public static void validate(MutationResponse r) {
        Objects.requireNonNull(r, "mutation response");
        RpcLimits.checkMessageSize(r); message(r.getMessage());
        requestId(r.getRequestId()); // required on every result, including failures
        switch (r.getCode()) {
            case RESULT_CODE_OK -> {
                if (!r.hasCommitIndex() || r.getCommitIndex() <= 0)
                    throw new IllegalArgumentException("successful mutation requires positive commit_index");
            }
            case RESULT_CODE_NOT_LEADER, RESULT_CODE_NO_QUORUM,
                 RESULT_CODE_REQUEST_ID_CONFLICT, RESULT_CODE_NOT_READY -> {
                if (r.hasCommitIndex()) throw new IllegalArgumentException("failed mutation cannot claim commit_index");
            }
            default -> throw new IllegalArgumentException("invalid mutation result code: " + r.getCode());
        }
        hintRules(r.getCode() == ResultCode.RESULT_CODE_NOT_LEADER, r.hasLeaderHint(), r.getLeaderHint());
    }

    public static void validate(MutationResponse response, PutRequest originatingRequest) {
        validate(originatingRequest); validate(response);
        if (!response.getRequestId().equals(originatingRequest.getRequestId()))
            throw new IllegalArgumentException("mutation response request_id mismatch");
    }

    public static void validate(MutationResponse response, DeleteRequest originatingRequest) {
        validate(originatingRequest); validate(response);
        if (!response.getRequestId().equals(originatingRequest.getRequestId()))
            throw new IllegalArgumentException("mutation response request_id mismatch");
    }

    public static void validate(GetResponse r) {
        Objects.requireNonNull(r, "get response");
        RpcLimits.checkMessageSize(r); message(r.getMessage()); key(r.getKey());
        // Configured, recovered, ready leader can read applied committed state without a NEW quorum.
        switch (r.getCode()) {
            case RESULT_CODE_OK -> {
                if (!r.hasValue()) throw new IllegalArgumentException("OK GET requires explicit value presence");
                RpcLimits.checkBytes(r.getValue().size(), 0, RpcLimits.MAX_VALUE_BYTES, "GET value");
                readIndex(r);
            }
            case RESULT_CODE_NOT_FOUND -> {
                if (r.hasValue()) throw new IllegalArgumentException("NOT_FOUND must not include value");
                readIndex(r);
            }
            case RESULT_CODE_NOT_LEADER, RESULT_CODE_NOT_READY -> {
                if (r.hasValue() || r.hasCommitIndex())
                    throw new IllegalArgumentException("non-read outcome must not include value/commit_index");
            }
            default -> throw new IllegalArgumentException("invalid GET outcome (NO_QUORUM is mutation-only): " + r.getCode());
        }
        hintRules(r.getCode() == ResultCode.RESULT_CODE_NOT_LEADER, r.hasLeaderHint(), r.getLeaderHint());
    }

    public static void validate(GetResponse response, GetRequest originatingRequest) {
        validate(originatingRequest); validate(response);
        if (!response.getKey().equals(originatingRequest.getKey()))
            throw new IllegalArgumentException("GET response key mismatch");
    }

    private static void readIndex(GetResponse r) {
        if (!r.hasCommitIndex() || r.getCommitIndex() < 0)
            throw new IllegalArgumentException("leader GET result needs nonnegative commit_index");
    }

    private static void hintRules(boolean needHint, boolean hasHint, LeaderHint hint) {
        if (needHint != hasHint) throw new IllegalArgumentException("leader_hint presence disagrees with result code");
        if (hasHint) {
            RpcLimits.validText(hint.getLeaderId(), RpcLimits.MAX_NODE_ID_BYTES, "leader_id", true);
            RpcLimits.validText(hint.getHost(), RpcLimits.MAX_HOST_BYTES, "host", true);
            if (hint.getPort() < 1 || hint.getPort() > 65535) throw new IllegalArgumentException("invalid leader TCP port");
        }
    }

    static void key(String value) { RpcLimits.validText(value, RpcLimits.MAX_KEY_BYTES, "key", false); }
    static void requestId(String value) { RpcLimits.validText(value, RpcLimits.MAX_REQUEST_ID_BYTES, "request_id", true); }
    private static void message(String value) {
        if (RpcLimits.strictUtf8Length(value, "message") > RpcLimits.MAX_MESSAGE_TEXT_BYTES)
            throw new IllegalArgumentException("message too long");
    }
}
