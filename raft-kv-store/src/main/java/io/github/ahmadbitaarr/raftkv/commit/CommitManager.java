package io.github.ahmadbitaarr.raftkv.commit;

import io.github.ahmadbitaarr.raftkv.log.LogEntry;
import io.github.ahmadbitaarr.raftkv.log.WalBackedPersistentLog;
import io.github.ahmadbitaarr.raftkv.wal.WalStore;
import java.io.IOException;
import java.util.Objects;
import java.util.function.LongConsumer;

/** The only leader COMMIT writer; publication follows a completed durable force. */
public final class CommitManager {
    private final AckTracker tracker;
    private final WalStore wal;
    private final WalBackedPersistentLog log;
    private final LongConsumer publish;
    public CommitManager(AckTracker tracker, WalStore wal, WalBackedPersistentLog log, LongConsumer publish) {
        this.tracker=Objects.requireNonNull(tracker);this.wal=Objects.requireNonNull(wal);
        this.log=Objects.requireNonNull(log);this.publish=Objects.requireNonNull(publish);
    }
    public void commit(LogEntry entry, AckTracker.MajorityCertificate certificate) throws IOException {
        Objects.requireNonNull(entry,"entry");
        if(!tracker.validates(certificate,entry)) throw new IllegalArgumentException("invalid matching-prefix majority certificate");
        long previous=wal.durableCommitIndex();
        if(previous==Long.MAX_VALUE || entry.index()!=previous+1
                || entry.index()>wal.lastDurableLogIndex() || entry.index()>log.lastLogIndex())
            throw new IllegalStateException("noncontiguous or unpersisted leader COMMIT");
        wal.appendCommitDurably(entry.index()); // FileChannel.force(true) before any publication.
        publish.accept(entry.index());
    }
}
