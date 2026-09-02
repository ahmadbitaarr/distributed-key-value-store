package io.github.ahmadbitaarr.raftkv.statemachine;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.ahmadbitaarr.raftkv.log.Command;
import java.util.List;
import org.junit.jupiter.api.Test;

class KvStateMachineTest {

    @Test
    void putNewKeyStoresValue() {
        KvStateMachine stateMachine = new KvStateMachine();

        stateMachine.apply(Command.put("name", new byte[] {1, 2, 3}));

        assertArrayEquals(new byte[] {1, 2, 3}, stateMachine.get("name").orElseThrow());
    }

    @Test
    void putExistingKeyOverwritesValue() {
        KvStateMachine stateMachine = new KvStateMachine();
        stateMachine.apply(Command.put("name", new byte[] {1}));

        stateMachine.apply(Command.put("name", new byte[] {2}));

        assertArrayEquals(new byte[] {2}, stateMachine.get("name").orElseThrow());
    }

    @Test
    void getExistingKeyReturnsValue() {
        KvStateMachine stateMachine = new KvStateMachine();
        stateMachine.apply(Command.put("key", new byte[] {10, 20}));

        assertArrayEquals(new byte[] {10, 20}, stateMachine.get("key").orElseThrow());
    }

    @Test
    void getMissingKeyReturnsEmpty() {
        KvStateMachine stateMachine = new KvStateMachine();

        assertTrue(stateMachine.get("missing").isEmpty());
    }

    @Test
    void deleteExistingKeyRemovesValue() {
        KvStateMachine stateMachine = new KvStateMachine();
        stateMachine.apply(Command.put("key", new byte[] {1}));

        stateMachine.apply(Command.delete("key"));

        assertTrue(stateMachine.get("key").isEmpty());
    }

    @Test
    void deleteMissingKeyIsNoOp() {
        KvStateMachine stateMachine = new KvStateMachine();

        stateMachine.apply(Command.delete("missing"));
        stateMachine.apply(Command.put("other", new byte[] {5}));

        assertTrue(stateMachine.get("missing").isEmpty());
        assertArrayEquals(new byte[] {5}, stateMachine.get("other").orElseThrow());
    }

    @Test
    void supportsArbitraryByteArrayValues() {
        KvStateMachine stateMachine = new KvStateMachine();
        byte[] binaryValue = {0x00, (byte) 0xFF, (byte) 0x80, 0x00, 0x7F};

        stateMachine.apply(Command.put("binary", binaryValue));

        assertArrayEquals(binaryValue, stateMachine.get("binary").orElseThrow());
    }

    @Test
    void putDefensivelyCopiesInputValue() {
        KvStateMachine stateMachine = new KvStateMachine();
        byte[] original = {1, 2, 3};
        Command command = Command.put("key", original);

        stateMachine.apply(command);
        original[0] = 99;

        assertArrayEquals(new byte[] {1, 2, 3}, stateMachine.get("key").orElseThrow());
    }

    @Test
    void getReturnsDefensiveCopy() {
        KvStateMachine stateMachine = new KvStateMachine();
        stateMachine.apply(Command.put("key", new byte[] {4, 5, 6}));

        byte[] returned = stateMachine.get("key").orElseThrow();
        returned[0] = 99;

        assertArrayEquals(new byte[] {4, 5, 6}, stateMachine.get("key").orElseThrow());
    }

    @Test
    void replayingSameCommandSequenceProducesSameState() {
        List<Command> commands = List.of(
                Command.put("alpha", new byte[] {1}),
                Command.put("beta", new byte[] {2}),
                Command.put("alpha", new byte[] {3}),
                Command.delete("beta"),
                Command.delete("missing"),
                Command.put("empty-value-key", new byte[0]));

        KvStateMachine first = new KvStateMachine();
        KvStateMachine second = new KvStateMachine();

        commands.forEach(first::apply);
        commands.forEach(second::apply);

        assertSameLookup(first, second, "alpha");
        assertSameLookup(first, second, "beta");
        assertSameLookup(first, second, "missing");
        assertSameLookup(first, second, "empty-value-key");

        assertArrayEquals(new byte[] {3}, first.get("alpha").orElseThrow());
        assertTrue(first.get("beta").isEmpty());
        assertTrue(first.get("missing").isEmpty());
        assertArrayEquals(new byte[0], first.get("empty-value-key").orElseThrow());
    }

    private static void assertSameLookup(KvStateMachine first, KvStateMachine second, String key) {
        var firstValue = first.get(key);
        var secondValue = second.get(key);

        assertTrue(firstValue.isPresent() == secondValue.isPresent());
        if (firstValue.isPresent()) {
            assertArrayEquals(firstValue.orElseThrow(), secondValue.orElseThrow());
        } else {
            assertFalse(secondValue.isPresent());
        }
    }
}
