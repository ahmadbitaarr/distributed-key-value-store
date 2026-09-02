package io.github.ahmadbitaarr.raftkv.log;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class CommandTest {

    @Test
    void putCommandDefensivelyCopiesInputValue() {
        byte[] original = {1, 2, 3};
        Command command = Command.put("key", original);

        original[0] = 99;

        assertArrayEquals(new byte[] {1, 2, 3}, command.value().orElseThrow());
    }

    @Test
    void commandValueReturnsDefensiveCopy() {
        Command command = Command.put("key", new byte[] {4, 5, 6});

        byte[] firstRead = command.value().orElseThrow();
        firstRead[1] = 99;

        assertArrayEquals(new byte[] {4, 5, 6}, command.value().orElseThrow());
    }

    @Test
    void commandsWithSameContentAreEqual() {
        Command first = Command.put("key", new byte[] {7, 8, 9});
        Command second = Command.put("key", new byte[] {7, 8, 9});
        Command different = Command.put("key", new byte[] {7, 8, 10});

        assertEquals(first, second);
        assertEquals(first.hashCode(), second.hashCode());
        assertNotEquals(first, different);
        assertEquals(Command.delete("key"), Command.delete("key"));
    }

    @Test
    void validatesCommandArguments() {
        assertThrows(NullPointerException.class, () -> Command.put(null, new byte[] {1}));
        assertThrows(IllegalArgumentException.class, () -> Command.put("", new byte[] {1}));
        assertThrows(NullPointerException.class, () -> Command.put("key", null));
        assertThrows(NullPointerException.class, () -> Command.delete(null));
        assertThrows(IllegalArgumentException.class, () -> Command.delete(""));
    }

    @Test
    void deleteCommandHasNoValue() {
        Command command = Command.delete("key");

        assertEquals(OperationType.DELETE, command.type());
        assertEquals("key", command.key());
        assertFalse(command.value().isPresent());
    }

    @Test
    void putAllowsEmptyByteArray() {
        Command command = Command.put("empty", new byte[0]);

        assertEquals(OperationType.PUT, command.type());
        assertTrue(command.value().isPresent());
        assertArrayEquals(new byte[0], command.value().orElseThrow());
    }
}
