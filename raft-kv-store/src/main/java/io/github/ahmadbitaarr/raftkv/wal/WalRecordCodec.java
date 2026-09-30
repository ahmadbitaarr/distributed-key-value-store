package io.github.ahmadbitaarr.raftkv.wal;

import io.github.ahmadbitaarr.raftkv.log.Command;
import io.github.ahmadbitaarr.raftkv.log.LogEntry;
import io.github.ahmadbitaarr.raftkv.log.OperationType;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.zip.CRC32C;

public final class WalRecordCodec {
    static final byte[] MAGIC = new byte[]{'R', 'K', 'V', 'W'};
    static final byte VERSION = 0x01;
    public static final int HEADER_SIZE = 10;
    static final int CHECKSUM_SIZE = Integer.BYTES;

    public static final int MAX_PAYLOAD_SIZE = 16 * 1024 * 1024;
    public static final int MAX_REQUEST_ID_BYTES = 1024;
    public static final int MAX_KEY_BYTES = 64 * 1024;
    public static final int MAX_VALUE_BYTES = 8 * 1024 * 1024;
    public static final int MAX_FINGERPRINT_BYTES = 1024;

    private static final byte PUT_CODE = 0x01;
    private static final byte DELETE_CODE = 0x02;

    public byte[] encode(WalRecord record) {
        if (record == null) {
            throw new NullPointerException("record must not be null");
        }
        byte[] payload = switch (record.type()) {
            case ENTRY -> encodeEntryPayload(record.entry());
            case COMMIT -> encodeCommitPayload(record.commitIndex());
        };
        return frame(record.type(), payload);
    }

    public WalRecord decode(byte[] framedRecord) throws WalCorruptionException {
        if (framedRecord == null) {
            throw new NullPointerException("framedRecord must not be null");
        }
        if (framedRecord.length < HEADER_SIZE + CHECKSUM_SIZE) {
            throw new WalCorruptionException("WAL record is shorter than the minimum frame size");
        }

        byte[] header = Arrays.copyOf(framedRecord, HEADER_SIZE);
        DecodedHeader decodedHeader = decodeHeader(header);
        int expectedLength = HEADER_SIZE + decodedHeader.payloadLength() + CHECKSUM_SIZE;
        if (framedRecord.length != expectedLength) {
            throw new WalCorruptionException(
                    "WAL frame length mismatch: expected " + expectedLength + " but was " + framedRecord.length);
        }

        verifyChecksum(framedRecord);
        byte[] payload = Arrays.copyOfRange(
                framedRecord, HEADER_SIZE, HEADER_SIZE + decodedHeader.payloadLength());
        return switch (decodedHeader.type()) {
            case ENTRY -> WalRecord.entry(decodeEntryPayload(payload));
            case COMMIT -> WalRecord.commit(decodeCommitPayload(payload));
        };
    }

    int payloadLengthFromHeader(byte[] header) throws WalCorruptionException {
        return decodeHeader(header).payloadLength();
    }

    static byte[] frameForTest(WalRecordType type, byte[] payload) {
        return frame(type, payload);
    }

    static void rewriteChecksumForTest(byte[] framedRecord) {
        if (framedRecord.length < CHECKSUM_SIZE) {
            throw new IllegalArgumentException("frame too small");
        }
        CRC32C crc32c = new CRC32C();
        crc32c.update(framedRecord, 0, framedRecord.length - CHECKSUM_SIZE);
        ByteBuffer.wrap(framedRecord)
                .order(ByteOrder.BIG_ENDIAN)
                .putInt(framedRecord.length - CHECKSUM_SIZE, (int) crc32c.getValue());
    }

    private static byte[] frame(WalRecordType type, byte[] payload) {
        if (type == null) {
            throw new NullPointerException("type must not be null");
        }
        if (payload == null) {
            throw new NullPointerException("payload must not be null");
        }
        requireLengthWithin(payload.length, 0, MAX_PAYLOAD_SIZE, "payload");

        ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE + payload.length + CHECKSUM_SIZE)
                .order(ByteOrder.BIG_ENDIAN);
        buffer.put(MAGIC);
        buffer.put(VERSION);
        buffer.put(type.code());
        buffer.putInt(payload.length);
        buffer.put(payload);

        CRC32C crc32c = new CRC32C();
        crc32c.update(buffer.array(), 0, HEADER_SIZE + payload.length);
        buffer.putInt((int) crc32c.getValue());
        return buffer.array();
    }

    private static DecodedHeader decodeHeader(byte[] header) throws WalCorruptionException {
        if (header.length != HEADER_SIZE) {
            throw new WalCorruptionException("invalid WAL header size: " + header.length);
        }
        ByteBuffer buffer = ByteBuffer.wrap(header).order(ByteOrder.BIG_ENDIAN);
        byte[] magic = new byte[MAGIC.length];
        buffer.get(magic);
        if (!Arrays.equals(MAGIC, magic)) {
            throw new WalCorruptionException("invalid WAL magic");
        }
        byte version = buffer.get();
        if (version != VERSION) {
            throw new WalCorruptionException("unsupported WAL version: " + Byte.toUnsignedInt(version));
        }
        WalRecordType type = WalRecordType.fromCode(buffer.get());
        int payloadLength = buffer.getInt();
        if (payloadLength < 0 || payloadLength > MAX_PAYLOAD_SIZE) {
            throw new WalCorruptionException("invalid WAL payload length: " + payloadLength);
        }
        return new DecodedHeader(type, payloadLength);
    }

    private static void verifyChecksum(byte[] frame) throws WalCorruptionException {
        int checksumOffset = frame.length - CHECKSUM_SIZE;
        long expected = Integer.toUnsignedLong(
                ByteBuffer.wrap(frame).order(ByteOrder.BIG_ENDIAN).getInt(checksumOffset));
        CRC32C crc32c = new CRC32C();
        crc32c.update(frame, 0, checksumOffset);
        long actual = crc32c.getValue();
        if (expected != actual) {
            throw new WalCorruptionException(
                    "WAL checksum mismatch: expected " + expected + " but calculated " + actual);
        }
    }

    private static byte[] encodeEntryPayload(LogEntry entry) {
        byte[] requestId = utf8(entry.requestId());
        byte[] key = utf8(entry.command().key());
        byte[] fingerprint = entry.commandFingerprint();
        byte[] value = entry.command().value().orElse(null);

        requireLengthWithin(requestId.length, 1, MAX_REQUEST_ID_BYTES, "requestId");
        requireLengthWithin(key.length, 1, MAX_KEY_BYTES, "key");
        requireLengthWithin(fingerprint.length, 1, MAX_FINGERPRINT_BYTES, "commandFingerprint");
        if (value != null) {
            requireLengthWithin(value.length, 0, MAX_VALUE_BYTES, "value");
        }

        boolean put = entry.command().type() == OperationType.PUT;
        if (put && value == null) {
            throw new IllegalArgumentException("PUT command must contain a value");
        }
        if (!put && value != null) {
            throw new IllegalArgumentException("DELETE command must not contain a value");
        }

        long payloadSize = Long.BYTES
                + Integer.BYTES + requestId.length
                + 1
                + Integer.BYTES + key.length
                + 1
                + (put ? Integer.BYTES + value.length : 0)
                + Integer.BYTES + fingerprint.length;
        if (payloadSize > MAX_PAYLOAD_SIZE) {
            throw new IllegalArgumentException("ENTRY payload exceeds maximum WAL payload size");
        }

        ByteBuffer buffer = ByteBuffer.allocate(Math.toIntExact(payloadSize)).order(ByteOrder.BIG_ENDIAN);
        buffer.putLong(entry.index());
        putLengthPrefixed(buffer, requestId);
        buffer.put(put ? PUT_CODE : DELETE_CODE);
        putLengthPrefixed(buffer, key);
        buffer.put(put ? (byte) 1 : (byte) 0);
        if (put) {
            putLengthPrefixed(buffer, value);
        }
        putLengthPrefixed(buffer, fingerprint);
        return buffer.array();
    }

    private static byte[] encodeCommitPayload(long commitIndex) {
        if (commitIndex < 0) {
            throw new IllegalArgumentException("commitIndex must not be negative");
        }
        return ByteBuffer.allocate(Long.BYTES)
                .order(ByteOrder.BIG_ENDIAN)
                .putLong(commitIndex)
                .array();
    }

    private static LogEntry decodeEntryPayload(byte[] payload) throws WalCorruptionException {
        try {
            ByteBuffer buffer = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
            long index = readLong(buffer, "index");
            if (index <= 0) {
                throw new WalCorruptionException("ENTRY index must be positive");
            }

            String requestId = decodeUtf8(readLengthPrefixed(
                    buffer, 1, MAX_REQUEST_ID_BYTES, "requestId"), "requestId");

            byte operationCode = readByte(buffer, "operationType");
            OperationType operationType = switch (operationCode) {
                case PUT_CODE -> OperationType.PUT;
                case DELETE_CODE -> OperationType.DELETE;
                default -> throw new WalCorruptionException(
                        "unknown operation type: " + Byte.toUnsignedInt(operationCode));
            };

            String key = decodeUtf8(readLengthPrefixed(buffer, 1, MAX_KEY_BYTES, "key"), "key");
            byte valuePresent = readByte(buffer, "valuePresent");

            Command command;
            if (operationType == OperationType.PUT) {
                if (valuePresent != 1) {
                    throw new WalCorruptionException("PUT ENTRY must mark value as present");
                }
                byte[] value = readLengthPrefixed(buffer, 0, MAX_VALUE_BYTES, "value");
                command = Command.put(key, value);
            } else {
                if (valuePresent != 0) {
                    throw new WalCorruptionException("DELETE ENTRY must not contain a value");
                }
                command = Command.delete(key);
            }

            byte[] fingerprint = readLengthPrefixed(
                    buffer, 1, MAX_FINGERPRINT_BYTES, "commandFingerprint");
            if (buffer.hasRemaining()) {
                throw new WalCorruptionException("ENTRY payload contains trailing bytes");
            }
            return new LogEntry(index, requestId, command, fingerprint);
        } catch (WalCorruptionException exception) {
            throw exception;
        } catch (RuntimeException exception) {
            throw new WalCorruptionException("malformed ENTRY payload", exception);
        }
    }

    private static long decodeCommitPayload(byte[] payload) throws WalCorruptionException {
        if (payload.length != Long.BYTES) {
            throw new WalCorruptionException("COMMIT payload must be exactly 8 bytes");
        }
        long commitIndex = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN).getLong();
        if (commitIndex < 0) {
            throw new WalCorruptionException("COMMIT index must not be negative");
        }
        return commitIndex;
    }

    private static byte[] readLengthPrefixed(
            ByteBuffer buffer, int minimumLength, int maximumLength, String name)
            throws WalCorruptionException {
        ensureRemaining(buffer, Integer.BYTES, name + " length");
        int length = buffer.getInt();
        if (length < minimumLength || length > maximumLength) {
            throw new WalCorruptionException("invalid " + name + " length: " + length);
        }
        ensureRemaining(buffer, length, name);
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return bytes;
    }

    private static long readLong(ByteBuffer buffer, String name) throws WalCorruptionException {
        ensureRemaining(buffer, Long.BYTES, name);
        return buffer.getLong();
    }

    private static byte readByte(ByteBuffer buffer, String name) throws WalCorruptionException {
        ensureRemaining(buffer, 1, name);
        return buffer.get();
    }

    private static void ensureRemaining(ByteBuffer buffer, int required, String name)
            throws WalCorruptionException {
        if (required < 0 || buffer.remaining() < required) {
            throw new WalCorruptionException("ENTRY payload truncated while reading " + name);
        }
    }

    private static String decodeUtf8(byte[] bytes, String name) throws WalCorruptionException {
        try {
            CharBuffer decoded = StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes));
            return decoded.toString();
        } catch (CharacterCodingException exception) {
            throw new WalCorruptionException("invalid UTF-8 in " + name, exception);
        }
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static void putLengthPrefixed(ByteBuffer buffer, byte[] bytes) {
        buffer.putInt(bytes.length);
        buffer.put(bytes);
    }

    private static void requireLengthWithin(int length, int minimum, int maximum, String name) {
        if (length < minimum || length > maximum) {
            throw new IllegalArgumentException(
                    name + " length must be between " + minimum + " and " + maximum + " bytes");
        }
    }

    private record DecodedHeader(WalRecordType type, int payloadLength) {
    }
}
