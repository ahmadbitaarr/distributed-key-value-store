package io.github.ahmadbitaarr.raftkv.log;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

public final class Command {
    private final OperationType type;
    private final String key;
    private final byte[] value;

    private Command(OperationType type, String key, byte[] value) {
        this.type = Objects.requireNonNull(type, "type must not be null");
        this.key = validateKey(key);
        this.value = value == null ? null : Arrays.copyOf(value, value.length);
    }

    public static Command put(String key, byte[] value) {
        Objects.requireNonNull(value, "value must not be null");
        return new Command(OperationType.PUT, key, value);
    }

    public static Command delete(String key) {
        return new Command(OperationType.DELETE, key, null);
    }

    public OperationType type() {
        return type;
    }

    public String key() {
        return key;
    }

    public Optional<byte[]> value() {
        if (value == null) {
            return Optional.empty();
        }
        return Optional.of(Arrays.copyOf(value, value.length));
    }

    private static String validateKey(String key) {
        Objects.requireNonNull(key, "key must not be null");
        if (key.isEmpty()) {
            throw new IllegalArgumentException("key must not be empty");
        }
        return key;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof Command command)) {
            return false;
        }
        return type == command.type
                && key.equals(command.key)
                && Arrays.equals(value, command.value);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(type, key);
        result = 31 * result + Arrays.hashCode(value);
        return result;
    }
}
