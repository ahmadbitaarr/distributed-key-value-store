package io.github.ahmadbitaarr.raftkv.leader;

import io.github.ahmadbitaarr.raftkv.log.Command;
import io.github.ahmadbitaarr.raftkv.log.OperationType;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.util.Objects;

/** v1 SHA-256 over explicitly framed, big-endian, UTF-8 command content. */
public final class CanonicalCommandFingerprint {
    private CanonicalCommandFingerprint() { }
    public static byte[] of(Command command) {
        Objects.requireNonNull(command);
        try {
            var encoded=new ByteArrayOutputStream();
            var out=new DataOutputStream(encoded);
            out.write(new byte[]{'R','A','F','T','K','V',7,1});
            out.writeByte(command.type()==OperationType.PUT?1:2);
            byte[] key=command.key().getBytes(StandardCharsets.UTF_8);
            out.writeInt(key.length);out.write(key);
            if(command.type()==OperationType.PUT) {
                byte[] value=command.value().orElseThrow();
                out.writeByte(1);out.writeInt(value.length);out.write(value);
            } else {
                out.writeByte(0);
            }
            out.flush();
            return MessageDigest.getInstance("SHA-256").digest(encoded.toByteArray());
        } catch(IOException | NoSuchAlgorithmException e) {throw new IllegalStateException("command fingerprint unavailable",e);}
    }
}
