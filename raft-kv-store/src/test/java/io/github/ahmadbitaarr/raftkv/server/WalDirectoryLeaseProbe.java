package io.github.ahmadbitaarr.raftkv.server;

import java.nio.file.Path;
/** Forked-process probe used only by WalDirectoryLeaseTest. */
public final class WalDirectoryLeaseProbe {
    private WalDirectoryLeaseProbe() { }
    public static void main(String[] args) {
        try (var ignored = WalDirectoryLease.acquire(Path.of(args[0]))) {
            System.exit(0);
        } catch (Exception expected) {
            System.exit(12);
        }
    }
}
