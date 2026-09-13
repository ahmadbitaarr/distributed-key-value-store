package io.github.ahmadbitaarr.raftkv.statemachine;

import io.github.ahmadbitaarr.raftkv.log.Command;
import io.github.ahmadbitaarr.raftkv.log.OperationType;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public final class KvStateMachine {
    private final Map<String, byte[]> state = new HashMap<>();

    public void apply(Command command) {
        Objects.requireNonNull(command, "command must not be null");

        if (command.type() == OperationType.PUT) {
            byte[] value = command.value().orElseThrow(
                    () -> new IllegalStateException("PUT command must contain a value"));
            state.put(command.key(), Arrays.copyOf(value, value.length));
            return;
        }

        if (command.type() == OperationType.DELETE) {
            state.remove(command.key());
            return;
        }

        throw new IllegalArgumentException("Unsupported operation type: " + command.type());
    }

    public Optional<byte[]> get(String key) {
        Objects.requireNonNull(key, "key must not be null");

        byte[] value = state.get(key);
        if (value == null) {
            return Optional.empty();
        }

        return Optional.of(Arrays.copyOf(value, value.length));
    }
}
