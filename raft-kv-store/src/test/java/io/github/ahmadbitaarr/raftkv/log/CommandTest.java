package io.github.ahmadbitaarr.raftkv.log;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class CommandTest {

    @Test
    void putCommandDefensivelyCopiesInputValue() {
        byte[] input = {1, 2, 3};
        Command command = Command.put("key", input);

        input[0] = 99;

        assertArrayEquals(new byte[]{1, 2, 3}, command.value().orElseThrow());
    }

    @Test
    void commandValueReturnsDefensiveCopy() {
        Command command = Command.put("key", new byte[]{1, 2, 3});

        byte[] firstRead = command.value().orElseThrow();
        firstRead[0] = 99;

        assertArrayEquals(new byte[]{1, 2, 3}, command.value().orElseThrow());
    }

    @Test
    void commandsWithSameContentAreEqual() {
        Command first = Command.put("key", new byte[]{1, 2, 3});
        Command second = Command.put("key", new byte[]{1, 2, 3});

        assertEquals(first, second);
        assertEquals(first.hashCode(), second.hashCode());

        Command deleteOne = Command.delete("key");
        Command deleteTwo = Command.delete("key");
        assertEquals(deleteOne, deleteTwo);
        assertEquals(deleteOne.hashCode(), deleteTwo.hashCode());
    }

    @Test
    void validatesCommandArguments() {
        assertThrows(NullPointerException.class, () -> Command.put(null, new byte[]{1}));
        assertThrows(IllegalArgumentException.class, () -> Command.put("", new byte[]{1}));
        assertThrows(NullPointerException.class, () -> Command.put("key", null));

        assertThrows(NullPointerException.class, () -> Command.delete(null));
        assertThrows(IllegalArgumentException.class, () -> Command.delete(""));
    }

    @Test
    void deleteCommandHasNoValue() {
        Command command = Command.delete("key");

        assertEquals(OperationType.DELETE, command.type());
        assertEquals("key", command.key());
        assertEquals(Optional.empty(), command.value());
    }

    @Test
    void putAllowsEmptyByteArray() {
        Command command = Command.put("key", new byte[0]);

        assertEquals(OperationType.PUT, command.type());
        assertTrue(command.value().isPresent());
        assertArrayEquals(new byte[0], command.value().orElseThrow());
    }
}
