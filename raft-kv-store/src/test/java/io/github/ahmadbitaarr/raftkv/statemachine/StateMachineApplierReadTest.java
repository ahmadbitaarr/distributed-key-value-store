package io.github.ahmadbitaarr.raftkv.statemachine;

import io.github.ahmadbitaarr.raftkv.log.*;
import io.github.ahmadbitaarr.raftkv.server.*;
import org.junit.jupiter.api.Test;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import static org.junit.jupiter.api.Assertions.*;

class StateMachineApplierReadTest {
    private static NodeStatusSnapshot status(long index) {
        return new NodeStatusSnapshot("n1",NodeRole.LEADER,NodeLifecycleState.READY,index,index,index,true);
    }
    private static LogEntry entry(int i,Command c){return new LogEntry(i,"r"+i,c,new byte[]{1});}

    @Test void synchronizedReadCannotObservePartialApplication() throws Exception {
        var applier=new StateMachineApplier(new KvStateMachine());
        var log=new InMemoryPersistentLog();log.append(entry(1,Command.put("k",new byte[]{2,4})));
        CountDownLatch insideGet=new CountDownLatch(1),releaseGet=new CountDownLatch(1),startingRead=new CountDownLatch(1);
        PersistentLog blocking=new PersistentLog() {
            @Override public void append(LogEntry e){throw new UnsupportedOperationException();}
            @Override public Optional<LogEntry> get(long i){
                insideGet.countDown();
                try {if(!releaseGet.await(5,TimeUnit.SECONDS))throw new IllegalStateException("test latch");}
                catch(InterruptedException e){Thread.currentThread().interrupt();throw new RuntimeException(e);}
                return log.get(i);
            }
            @Override public List<LogEntry> getRange(long a,long b){return log.getRange(a,b);}
            @Override public long lastLogIndex(){return log.lastLogIndex();}
            @Override public void truncateAfter(long i){throw new UnsupportedOperationException();}
        };
        var state=new AtomicReference<>(new NodeStatusSnapshot("n1",NodeRole.LEADER,
                NodeLifecycleState.READY,1,1,0,true));
        ExecutorService workers=Executors.newFixedThreadPool(2);
        try {
            var writer=workers.submit(()->applier.applyThrough(1,1,blocking));
            assertTrue(insideGet.await(5,TimeUnit.SECONDS));
            var reader=workers.submit(()->{
                startingRead.countDown();
                return applier.readCommitted("k",state::get);
            });
            assertTrue(startingRead.await(5,TimeUnit.SECONDS));
            assertFalse(reader.isDone(),"GET must wait until application releases its lock");
            releaseGet.countDown();
            assertEquals(1L,writer.get(5,TimeUnit.SECONDS).longValue());
            assertTrue(reader.get(5,TimeUnit.SECONDS).isEmpty(),
                    "applied 1 and published 0 must yield NOT_READY");
            state.set(status(1));
            assertArrayEquals(new byte[]{2,4},applier.readCommitted("k",state::get)
                    .orElseThrow().value().orElseThrow());
        }finally{releaseGet.countDown();workers.shutdownNow();assertTrue(workers.awaitTermination(5,TimeUnit.SECONDS));}
    }
    @Test void synchronizedReadDoesNotReadAfterApplicationFailure() {
        var applier=new StateMachineApplier(new KvStateMachine());
        // Deliberately missing log.get(1), with lastLogIndex deliberately inconsistent via a wrapper.
        PersistentLog broken=new PersistentLog(){
            public void append(LogEntry e){throw new UnsupportedOperationException();}
            public Optional<LogEntry> get(long i){return Optional.empty();}
            public List<LogEntry> getRange(long a,long b){return List.of();}
            public long lastLogIndex(){return 1;}
            public void truncateAfter(long i){throw new UnsupportedOperationException();}
        };
        assertThrows(NoSuchElementException.class,()->applier.applyThrough(1,1,broken));
        assertTrue(applier.failed());
        assertTrue(applier.readCommitted("x",()->status(0)).isEmpty());
    }
    @Test void defensiveCopiesAndEmptyPresentBytes() {
        var applier=new StateMachineApplier(new KvStateMachine());var log=new InMemoryPersistentLog();
        log.append(entry(1,Command.put("x",new byte[0])));
        applier.applyThrough(1,1,log);
        var empty=applier.readCommitted("x",()->status(1)).orElseThrow();
        assertTrue(empty.value().isPresent());assertEquals(0,empty.value().orElseThrow().length);
        log.append(entry(2,Command.put("x",new byte[]{1,0,2})));
        applier.applyThrough(2,2,log);
        var value=applier.readCommitted("x",()->status(2)).orElseThrow();
        value.value().orElseThrow()[1]=99;
        assertArrayEquals(new byte[]{1,0,2},value.value().orElseThrow());
        assertArrayEquals(new byte[]{1,0,2},applier.readCommitted("x",()->status(2)).orElseThrow().value().orElseThrow());
    }
}
