package io.github.ahmadbitaarr.raftkv.log;

import io.github.ahmadbitaarr.raftkv.wal.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

class Phase9LogSnapshotConcurrencyTest {
    @TempDir Path directory;
    private static LogEntry entry(long index){return new LogEntry(index,"id-"+index,
            Command.put("k",new byte[]{(byte)index}),new byte[]{(byte)index});}
    @Test void boundedLogSnapshotWaitsForDurableAppendAndDoesNotExposeMutableBackingList()throws Exception{
        var written=new CountDownLatch(1);var release=new CountDownLatch(1);
        try(var actual=FileWalStore.open(directory.resolve("raftkv.wal"))){
            WalStore blocking=new WalStore(){
                public WalRecoveryResult recoveryResult(){return actual.recoveryResult();}
                public void appendEntryDurably(LogEntry e)throws IOException{
                    if(e.index()==2){written.countDown();
                        try {if(!release.await(3,TimeUnit.SECONDS))throw new IOException("blocked force timeout");}
                        catch(InterruptedException x){Thread.currentThread().interrupt();throw new IOException(x);}}
                    actual.appendEntryDurably(e);
                }
                public void appendCommitDurably(long n)throws IOException{actual.appendCommitDurably(n);}
                public void rewriteDurably(List<LogEntry> e,long n)throws IOException{actual.rewriteDurably(e,n);}
                public long lastDurableLogIndex(){return actual.lastDurableLogIndex();}
                public long durableCommitIndex(){return actual.durableCommitIndex();}
                public void close()throws IOException{actual.close();}
            };
            try(var log=new WalBackedPersistentLog(blocking)){
                log.append(entry(1));
                var pool=Executors.newFixedThreadPool(2);
                try {
                    Future<?> append=pool.submit(()->{try{log.append(entry(2));}catch(IOException e){throw new RuntimeException(e);}});
                    assertTrue(written.await(3,TimeUnit.SECONDS));
                    var enteringSnapshot=new CountDownLatch(1);
                    Future<List<LogEntry>> snapshot=pool.submit(()->{
                        enteringSnapshot.countDown();return log.snapshotRange(1,2,2);
                    });
                    assertTrue(enteringSnapshot.await(3,TimeUnit.SECONDS));
                    assertThrows(TimeoutException.class,()->snapshot.get(50,TimeUnit.MILLISECONDS),
                            "snapshot must not advance before append force returns");
                    release.countDown();append.get(3,TimeUnit.SECONDS);
                    var contents=snapshot.get(3,TimeUnit.SECONDS);
                    assertEquals(List.of(entry(1),entry(2)),contents);
                    assertThrows(UnsupportedOperationException.class,()->contents.clear());
                    assertEquals(2,actual.lastDurableLogIndex());
                } finally {release.countDown();pool.shutdownNow();}
            }
        }
    }
}
