package io.github.ahmadbitaarr.raftkv.server;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class Phase8ReadinessSnapshotTest {
    @Test void onlyReadyConfiguredLeaderMayReportReady() {
        assertThrows(IllegalArgumentException.class,()->new NodeStatusSnapshot("n2",NodeRole.FOLLOWER,NodeLifecycleState.READY,0,0,0,true));
        assertThrows(IllegalArgumentException.class,()->new NodeStatusSnapshot("n1",NodeRole.LEADER,NodeLifecycleState.READY,0,0,0,false));
        assertThrows(IllegalArgumentException.class,()->new NodeStatusSnapshot("n1",NodeRole.LEADER,NodeLifecycleState.FAILED,0,0,0,true));
        assertThrows(IllegalArgumentException.class,()->new NodeStatusSnapshot("n1",NodeRole.LEADER,NodeLifecycleState.CONTROL_ONLY,0,0,0,true));
        assertTrue(new NodeStatusSnapshot("n1",NodeRole.LEADER,NodeLifecycleState.READY,0,0,0,true).ready());
    }
    @Test void readyTransitionAndFailuresUpdateReadinessWithoutLosingIndexes() {
        var recovered=new NodeStatusSnapshot("n1",NodeRole.LEADER,NodeLifecycleState.CONTROL_ONLY,2,1,1,false);
        var ready=recovered.withState(NodeLifecycleState.READY);
        assertTrue(ready.ready());assertEquals(2,ready.lastLogIndex());assertEquals(1,ready.appliedIndex());
        for(var end:new NodeLifecycleState[]{NodeLifecycleState.STOPPING,NodeLifecycleState.FAILED,NodeLifecycleState.STOPPED}){
            var after=ready.withState(end);
            assertFalse(after.ready());assertEquals(1,after.appliedIndex());assertEquals(1,after.durableCommitIndex());
        }
    }
    @Test void allStatusIndexInvariantsRemainEnforced() {
        assertThrows(IllegalArgumentException.class,()->new NodeStatusSnapshot("n1",NodeRole.LEADER,NodeLifecycleState.READY,1,2,0,true));
        assertThrows(IllegalArgumentException.class,()->new NodeStatusSnapshot("n1",NodeRole.LEADER,NodeLifecycleState.READY,2,1,2,true));
    }
}
