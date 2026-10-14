package io.github.ahmadbitaarr.raftkv.rpc;

import com.google.protobuf.ByteString;
import io.github.ahmadbitaarr.raftkv.log.Command;
import io.github.ahmadbitaarr.raftkv.log.LogEntry;
import io.github.ahmadbitaarr.raftkv.log.OperationType;
import java.util.Objects;

/** Strict stateless boundary between immutable Java domain data and generated protobuf data. */
public final class ProtoMapper {
    private ProtoMapper() { }

    public static io.github.ahmadbitaarr.raftkv.rpc.generated.replication.v1.OperationType toProtoOperation(OperationType type) {
        Objects.requireNonNull(type, "type");
        return switch (type) {
            case PUT -> io.github.ahmadbitaarr.raftkv.rpc.generated.replication.v1.OperationType.OPERATION_TYPE_PUT;
            case DELETE -> io.github.ahmadbitaarr.raftkv.rpc.generated.replication.v1.OperationType.OPERATION_TYPE_DELETE;
        };
    }

    public static OperationType toDomainOperation(io.github.ahmadbitaarr.raftkv.rpc.generated.replication.v1.OperationType type) {
        Objects.requireNonNull(type, "type");
        return switch (type) {
            case OPERATION_TYPE_PUT -> OperationType.PUT;
            case OPERATION_TYPE_DELETE -> OperationType.DELETE;
            default -> throw new IllegalArgumentException("unspecified/unknown replicated operation: " + type);
        };
    }

    public static Command toDomainCommand(
            io.github.ahmadbitaarr.raftkv.rpc.generated.replication.v1.OperationType type,
            String key, boolean hasValue, ByteString bytes) {
        Objects.requireNonNull(bytes, "bytes");
        RpcLimits.validText(key, RpcLimits.MAX_KEY_BYTES, "key", false);
        var operation = toDomainOperation(type);
        return switch (operation) {
            case PUT -> {
                if (!hasValue) throw new IllegalArgumentException("PUT value must be explicitly present");
                RpcLimits.checkBytes(bytes.size(), 0, RpcLimits.MAX_VALUE_BYTES, "PUT value");
                yield Command.put(key, bytes.toByteArray());
            }
            case DELETE -> {
                if (hasValue || !bytes.isEmpty()) throw new IllegalArgumentException("DELETE must not carry a value");
                yield Command.delete(key);
            }
        };
    }

    public static io.github.ahmadbitaarr.raftkv.rpc.generated.replication.v1.LogEntry toProtoLogEntry(LogEntry entry) {
        Objects.requireNonNull(entry, "entry");
        if (entry.index() <= 0) throw new IllegalArgumentException("index must be positive");
        RpcLimits.validText(entry.requestId(), RpcLimits.MAX_REQUEST_ID_BYTES, "request_id", true);
        RpcLimits.validText(entry.command().key(), RpcLimits.MAX_KEY_BYTES, "key", false);
        byte[] fingerprint = entry.commandFingerprint();
        RpcLimits.checkBytes(fingerprint.length, 1, RpcLimits.MAX_FINGERPRINT_BYTES, "fingerprint");
        var out = io.github.ahmadbitaarr.raftkv.rpc.generated.replication.v1.LogEntry.newBuilder()
                .setIndex(entry.index()).setRequestId(entry.requestId()).setKey(entry.command().key())
                .setOperation(toProtoOperation(entry.command().type()))
                .setCommandFingerprint(ByteString.copyFrom(fingerprint));
        if (entry.command().type() == OperationType.PUT) {
            byte[] value = entry.command().value().orElseThrow();
            RpcLimits.checkBytes(value.length, 0, RpcLimits.MAX_VALUE_BYTES, "value");
            out.setValue(ByteString.copyFrom(value)); // preserve presence for empty PUT bytes
        }
        var proto = out.build();
        RpcLimits.checkMessageSize(proto);
        return proto;
    }

    public static LogEntry toDomainLogEntry(io.github.ahmadbitaarr.raftkv.rpc.generated.replication.v1.LogEntry proto) {
        Objects.requireNonNull(proto, "proto");
        ReplicationContractValidator.validate(proto);
        Command command = toDomainCommand(proto.getOperation(), proto.getKey(), proto.hasValue(), proto.getValue());
        return new LogEntry(proto.getIndex(), proto.getRequestId(), command, proto.getCommandFingerprint().toByteArray());
    }
}
