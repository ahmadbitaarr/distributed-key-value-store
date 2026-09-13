package io.github.ahmadbitaarr.raftkv.statemachine;

import io.github.ahmadbitaarr.raftkv.log.Command;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class KvStateMachineTest {

    @Test
    void putNewKeyStoresValue() {
        KvStateMachine stateMachine = new KvStateMachine();

        stateMachine.apply(Command.put("alpha", new byte[]{1, 2, 3}));

        assertArrayEquals(new byte[]{1, 2, 3}, stateMachine.get("alpha").orElseThrow());
    }

    @Test
    void putExistingKeyOverwritesValue() {
        KvStateMachine stateMachine = new KvStateMachine();
        stateMachine.apply(Command.put("alpha", new byte[]{1}));

        stateMachine.apply(Command.put("alpha", new byte[]{2, 3}));

        assertArrayEquals(new byte[]{2, 3}, stateMachine.get("alpha").orElseThrow());
    }

    @Test
    void getExistingKeyReturnsValue() {
        KvStateMachine stateMachine = new KvStateMachine();
        stateMachine.apply(Command.put("alpha", new byte[]{7, 8}));

        assertArrayEquals(new byte[]{7, 8}, stateMachine.get("alpha").orElseThrow());
    }

    @Test
    void getMissingKeyReturnsEmpty() {
        KvStateMachine stateMachine = new KvStateMachine();

        assertTrue(stateMachine.get("missing").isEmpty());
    }

    @Test
    void deleteExistingKeyRemovesValue() {
        KvStateMachine stateMachine = new KvStateMachine();
        stateMachine.apply(Command.put("alpha", new byte[]{1}));

        stateMachine.apply(Command.delete("alpha"));

        assertTrue(stateMachine.get("alpha").isEmpty());
    }

    @Test
    void deleteMissingKeyIsNoOp() {
        KvStateMachine stateMachine = new KvStateMachine();

        assertDoesNotThrow(() -> stateMachine.apply(Command.delete("missing")));
        assertTrue(stateMachine.get("missing").isEmpty());

        stateMachine.apply(Command.put("still-works", new byte[]{4}));
        assertArrayEquals(new byte[]{4}, stateMachine.get("still-works").orElseThrow());
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
        byte[] input = {1, 2, 3};

        stateMachine.apply(Command.put("alpha", input));
        input[0] = 99;

        assertArrayEquals(new byte[]{1, 2, 3}, stateMachine.get("alpha").orElseThrow());
    }

    @Test
    void getReturnsDefensiveCopy() {
        KvStateMachine stateMachine = new KvStateMachine();
        stateMachine.apply(Command.put("alpha", new byte[]{1, 2, 3}));

        byte[] returned = stateMachine.get("alpha").orElseThrow();
        returned[0] = 99;

        assertArrayEquals(new byte[]{1, 2, 3}, stateMachine.get("alpha").orElseThrow());
    }

    @Test
    void replayingSameCommandSequenceProducesSameState() {
        List<Command> commands = List.of(
                Command.put("alpha", new byte[]{1}),
                Command.put("beta", new byte[]{2}),
                Command.put("alpha", new byte[]{3}),
                Command.delete("beta"),
                Command.delete("missing"),
                Command.put("empty", new byte[0])
        );

        KvStateMachine first = new KvStateMachine();
        KvStateMachine second = new KvStateMachine();

        commands.forEach(first::apply);
        commands.forEach(second::apply);

        assertArrayEquals(first.get("alpha").orElseThrow(), second.get("alpha").orElseThrow());
        assertEquals(first.get("beta").isPresent(), second.get("beta").isPresent());
        assertEquals(first.get("missing").isPresent(), second.get("missing").isPresent());
        assertArrayEquals(first.get("empty").orElseThrow(), second.get("empty").orElseThrow());

        assertArrayEquals(new byte[]{3}, first.get("alpha").orElseThrow());
        assertTrue(first.get("beta").isEmpty());
        assertTrue(first.get("missing").isEmpty());
        assertArrayEquals(new byte[0], first.get("empty").orElseThrow());
    }
}
