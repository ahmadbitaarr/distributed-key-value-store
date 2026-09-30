package io.github.ahmadbitaarr.raftkv.wal;

import java.io.IOException;

public final class WalCorruptionException extends IOException {
    public WalCorruptionException(String message) {
        super(message);
    }

    public WalCorruptionException(String message, Throwable cause) {
        super(message, cause);
    }
}
