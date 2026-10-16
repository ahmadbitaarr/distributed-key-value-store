package io.github.ahmadbitaarr.raftkv.statemachine;

import io.github.ahmadbitaarr.raftkv.log.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class StateMachineApplierTest {
    private LogEntry entry(long n,Command c){return new LogEntry(n,"id-"+n,c,new byte[]{(byte)n});}
    @Test void neverApplyUncommittedEntry() throws Exception {
        var log=new InMemoryPersistentLog();log.append(entry(1,Command.put("k",new byte[]{1})));
        var applier=new StateMachineApplier(new KvStateMachine());
        assertThrows(IllegalArgumentException.class,()->applier.applyThrough(1,0,log));
        assertTrue(applier.diagnosticGetForTest("k").isEmpty());
    }
    @Test void applyExactOrderOnlyOnce() throws Exception {
        var log=new InMemoryPersistentLog();log.append(entry(1,Command.put("k",new byte[]{1})));
        log.append(entry(2,Command.put("k",new byte[]{2})));
        var applier=new StateMachineApplier(new KvStateMachine());
        assertEquals(2,applier.applyThrough(2,2,log));assertArrayEquals(new byte[]{2},applier.diagnosticGetForTest("k").orElseThrow());
        assertEquals(2,applier.applyThrough(2,2,log));assertThrows(IllegalArgumentException.class,()->applier.applyThrough(1,2,log));
    }
    @Test void deleteIsAppliedOnlyAfterCommit() throws Exception {
        var log=new InMemoryPersistentLog();log.append(entry(1,Command.put("x",new byte[0])));log.append(entry(2,Command.delete("x")));
        var applier=new StateMachineApplier(new KvStateMachine());applier.applyThrough(1,1,log);
        assertEquals(0,applier.diagnosticGetForTest("x").orElseThrow().length);
        applier.applyThrough(2,2,log);assertTrue(applier.diagnosticGetForTest("x").isEmpty());
    }
}
