package io.github.ahmadbitaarr.raftkv.log;

import java.util.Arrays;
import java.util.Objects;

public final class LogEntry {
    private final long index;
    private final String requestId;
    private final Command command;
    private final byte[] commandFingerprint;

    public LogEntry(long index, String requestId, Command command, byte[] commandFingerprint) {
        if (index <= 0) {
            throw new IllegalArgumentException("index must be positive");
        }

        this.requestId = validateRequestId(requestId);
        this.command = Objects.requireNonNull(command, "command must not be null");
        this.commandFingerprint = copyAndValidateFingerprint(commandFingerprint);
        this.index = index;
    }

    public long index() {
        return index;
    }

    public String requestId() {
        return requestId;
    }

    public Command command() {
        return command;
    }

    public byte[] commandFingerprint() {
        return Arrays.copyOf(commandFingerprint, commandFingerprint.length);
    }

    private static String validateRequestId(String requestId) {
        Objects.requireNonNull(requestId, "requestId must not be null");
        if (requestId.isBlank()) {
            throw new IllegalArgumentException("requestId must not be blank");
        }
        return requestId;
    }

    private static byte[] copyAndValidateFingerprint(byte[] commandFingerprint) {
        Objects.requireNonNull(commandFingerprint, "commandFingerprint must not be null");
        if (commandFingerprint.length == 0) {
            throw new IllegalArgumentException("commandFingerprint must not be empty");
        }
        return Arrays.copyOf(commandFingerprint, commandFingerprint.length);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof LogEntry logEntry)) {
            return false;
        }
        return index == logEntry.index
                && requestId.equals(logEntry.requestId)
                && command.equals(logEntry.command)
                && Arrays.equals(commandFingerprint, logEntry.commandFingerprint);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(index, requestId, command);
        result = 31 * result + Arrays.hashCode(commandFingerprint);
        return result;
    }
}
