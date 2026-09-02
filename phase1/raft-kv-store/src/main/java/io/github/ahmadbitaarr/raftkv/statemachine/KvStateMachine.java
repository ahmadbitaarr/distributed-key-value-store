package io.github.ahmadbitaarr.raftkv.statemachine;

import io.github.ahmadbitaarr.raftkv.log.Command;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public final class KvStateMachine {
    private final Map<String, byte[]> data = new HashMap<>();

    public void apply(Command command) {
        Objects.requireNonNull(command, "command must not be null");

        switch (command.type()) {
            case PUT -> {
                byte[] value = command.value().orElseThrow(
                        () -> new IllegalStateException("PUT command must contain a value"));
                data.put(command.key(), Arrays.copyOf(value, value.length));
            }
            case DELETE -> data.remove(command.key());
        }
    }

    public Optional<byte[]> get(String key) {
        Objects.requireNonNull(key, "key must not be null");

        byte[] value = data.get(key);
        if (value == null) {
            return Optional.empty();
        }
        return Optional.of(Arrays.copyOf(value, value.length));
    }
}
