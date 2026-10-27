package io.github.ahmadbitaarr.raftkv.recovery;

import io.github.ahmadbitaarr.raftkv.leader.CanonicalCommandFingerprint;
import io.github.ahmadbitaarr.raftkv.log.*;
import io.github.ahmadbitaarr.raftkv.statemachine.*;
import io.github.ahmadbitaarr.raftkv.wal.*;
import java.nio.file.Path;

/** Separate JVM crash-boundary fixture. Runtime.halt deliberately avoids Java shutdown hooks. */
public final class RecoveryCrashProbe {
    public static void main(String[] args)throws Exception {
        if(args.length!=2)throw new IllegalArgumentException("mode and WAL path required");
        var c=Command.put("crash",new byte[]{9,0,4});
        var entry=new LogEntry(1,"request-before-crash",c,CanonicalCommandFingerprint.of(c));
        FileWalStore wal=FileWalStore.open(Path.of(args[1]));
        wal.appendEntryDurably(entry);
        if(!args[0].equals("ENTRY_ONLY")) {
            wal.appendCommitDurably(1);
            if(args[0].equals("AFTER_APPLY")) {
                var applier=new StateMachineApplier(new KvStateMachine());
                applier.applyThrough(1,1,new WalBackedPersistentLog(wal));
                if(applier.appliedIndex()!=1)throw new AssertionError("apply incomplete");
            }
        }
        // FileChannel.force(true) has returned from the requested durable boundary.
        Runtime.getRuntime().halt(0);
    }
}
