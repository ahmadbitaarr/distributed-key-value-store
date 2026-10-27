package io.github.ahmadbitaarr.raftkv.wal;

import io.github.ahmadbitaarr.raftkv.leader.CanonicalCommandFingerprint;
import io.github.ahmadbitaarr.raftkv.log.Command;
import io.github.ahmadbitaarr.raftkv.log.LogEntry;
import io.github.ahmadbitaarr.raftkv.log.WalBackedPersistentLog;
import java.nio.file.*;

/** Child-JVM fault boundaries inside real file-backed WAL code; no shutdown hooks run. */
public final class Phase10WalCrashBoundaryProbe {
    private Phase10WalCrashBoundaryProbe() { }
    private static LogEntry entry(int index) {
        Command command=Command.put("crash-"+index,new byte[]{(byte)index,0});
        return new LogEntry(index,"wal-boundary-"+index,command,CanonicalCommandFingerprint.of(command));
    }
    public static void main(String[] args) throws Exception {
        if(args.length!=2)throw new IllegalArgumentException("mode and WAL path required");
        Path path=Path.of(args[1]);
        switch(args[0]) {
            case "BEFORE_ENTRY_FORCE" -> {
                // The ENTRY frame is written before this callback, but force(true) NEVER runs.
                // A kernel may retain those bytes; no COMMIT may be inferred from them.
                try(FileWalStore wal=FileWalStore.openForTest(path,
                        channel -> Runtime.getRuntime().halt(41),
                        FileWalStore.productionAtomicMover())) {
                    wal.appendEntryDurably(entry(1));
                }
                throw new AssertionError("entry force boundary did not halt");
            }
            case "REWRITE_BEFORE_MOVE", "REWRITE_AFTER_MOVE" -> {
                try(FileWalStore wal=FileWalStore.open(path)) {
                    wal.appendEntryDurably(entry(1));
                    wal.appendCommitDurably(1);
                    wal.appendEntryDurably(entry(2)); // durable, but never committed
                }
                FileWalStore.AtomicMover mover=(src,dst) -> {
                    if(args[0].equals("REWRITE_BEFORE_MOVE")) Runtime.getRuntime().halt(42);
                    FileWalStore.productionAtomicMover().move(src,dst);
                    // The replacement has happened, but reopen/validation has not.
                    Runtime.getRuntime().halt(43);
                };
                try(FileWalStore wal=FileWalStore.openForTest(path,
                        FileWalStore.productionForcer(),mover)) {
                    new WalBackedPersistentLog(wal).truncateAfter(1);
                }
                throw new AssertionError("rewrite boundary did not halt");
            }
            default -> throw new IllegalArgumentException("unknown mode");
        }
    }
}
