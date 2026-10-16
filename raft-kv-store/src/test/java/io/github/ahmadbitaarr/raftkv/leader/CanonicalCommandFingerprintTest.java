package io.github.ahmadbitaarr.raftkv.leader;

import io.github.ahmadbitaarr.raftkv.log.Command;
import org.junit.jupiter.api.Test;
import java.util.Arrays;
import static org.junit.jupiter.api.Assertions.*;

class CanonicalCommandFingerprintTest {
    @Test void deterministicLengthAndStableHash(){assertArrayEquals(CanonicalCommandFingerprint.of(Command.put("é",new byte[]{1,2})),CanonicalCommandFingerprint.of(Command.put("é",new byte[]{1,2})));assertEquals(32,CanonicalCommandFingerprint.of(Command.delete("x")).length);}
    @Test void emptyPutIsNotDelete(){assertFalse(Arrays.equals(CanonicalCommandFingerprint.of(Command.put("x",new byte[0])),CanonicalCommandFingerprint.of(Command.delete("x"))));}
    @Test void keyValueBoundaryIsUnambiguous(){assertFalse(Arrays.equals(CanonicalCommandFingerprint.of(Command.put("ab",new byte[]{1})),CanonicalCommandFingerprint.of(Command.put("a",new byte[]{'b',1}))));}
    @Test void operationAndValueChangeFingerprint(){assertFalse(Arrays.equals(CanonicalCommandFingerprint.of(Command.put("x",new byte[]{1})),CanonicalCommandFingerprint.of(Command.put("x",new byte[]{2}))));}
}
