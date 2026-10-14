package io.github.ahmadbitaarr.raftkv.rpc;

import com.google.protobuf.MessageLite;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/** Wire-contract limits; transport builders in later phases must use these inbound budgets. */
public final class RpcLimits {
    public static final int MAX_REQUEST_ID_BYTES = 1024;
    public static final int MAX_KEY_BYTES = 65_536;
    public static final int MAX_VALUE_BYTES = 8_388_608;
    public static final int MAX_FINGERPRINT_BYTES = 1024;
    public static final int MAX_NODE_ID_BYTES = 256;
    public static final int MAX_HOST_BYTES = 253;
    public static final int MAX_MESSAGE_TEXT_BYTES = 2048;
    public static final int MAX_SERIALIZED_MESSAGE_BYTES = 12_582_912;
    public static final int CLIENT_INBOUND_MESSAGE_BYTES = MAX_SERIALIZED_MESSAGE_BYTES;
    public static final int SERVER_INBOUND_MESSAGE_BYTES = MAX_SERIALIZED_MESSAGE_BYTES;

    private RpcLimits() { }

    /** Validate a nonempty text field using strict UTF-8 encoding, not Java char count. */
    public static void validText(String text, int maxUtf8Bytes, String field, boolean requireNonBlank) {
        Objects.requireNonNull(text, field + " must not be null");
        if (text.isEmpty() || (requireNonBlank && text.isBlank())) {
            throw new IllegalArgumentException(field + " must not be empty" + (requireNonBlank ? " or blank" : ""));
        }
        if (strictUtf8Length(text, field) > maxUtf8Bytes) {
            throw new IllegalArgumentException(field + " exceeds " + maxUtf8Bytes + " UTF-8 bytes");
        }
    }

    public static int strictUtf8Length(String text, String field) {
        Objects.requireNonNull(text, field + " must not be null");
        var encoder = StandardCharsets.UTF_8.newEncoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT);
        try {
            ByteBuffer bytes = encoder.encode(CharBuffer.wrap(text));
            return bytes.remaining();
        } catch (CharacterCodingException e) {
            throw new IllegalArgumentException(field + " contains malformed Unicode", e);
        }
    }

    public static void checkBytes(int count, int min, int max, String field) {
        if (count < min || count > max) {
            throw new IllegalArgumentException(field + " must contain " + min + ".." + max + " bytes");
        }
    }

    public static void checkMessageSize(MessageLite message) {
        Objects.requireNonNull(message, "message");
        if (message.getSerializedSize() > MAX_SERIALIZED_MESSAGE_BYTES) {
            throw new IllegalArgumentException("protobuf message exceeds 12 MiB serialized budget");
        }
    }
}
