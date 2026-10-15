package io.github.ahmadbitaarr.raftkv.server;

import java.io.IOException;
import java.nio.channels.*;
import java.nio.file.*;
import static java.nio.file.StandardOpenOption.*;
/** Process-local/filesystem best-effort ownership lock; never a distributed lease. */
public final class WalDirectoryLease implements AutoCloseable {
    private final FileChannel channel;
    private final FileLock lock;
    private boolean closed;
    private WalDirectoryLease(FileChannel channel,FileLock lock){this.channel=channel;this.lock=lock;}
    public static WalDirectoryLease acquire(Path directory) throws IOException {
        Path path=directory.toAbsolutePath().normalize();
        Files.createDirectories(path);
        FileChannel channel=FileChannel.open(path.resolve("node.lock"),CREATE,WRITE);
        try {
            FileLock lock;
            try{lock=channel.tryLock();}catch(OverlappingFileLockException e){throw new IOException("WAL directory already owned",e);}
            if(lock==null)throw new IOException("cannot acquire exclusive WAL directory lease");
            return new WalDirectoryLease(channel,lock);
        }catch(IOException | RuntimeException failure){try{channel.close();}catch(IOException e){failure.addSuppressed(e);}throw failure;}
    }
    public boolean isHeld(){return !closed && lock.isValid();}
    @Override public synchronized void close() throws IOException {
        if(closed)return;
        IOException failed=null;
        try{if(lock.isValid())lock.release();}catch(IOException e){failed=e;}
        try{channel.close();}catch(IOException e){if(failed==null)failed=e;else failed.addSuppressed(e);}
        if(failed==null)closed=true;
        else throw failed;
    }
}
