package io.github.ahmadbitaarr.raftkv.server;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
class NodeLifecycleStateTest {
 @Test void followsAllowedStartupTransitions(){assertTrue(NodeLifecycleState.NEW.canTransitionTo(NodeLifecycleState.STARTING));assertTrue(NodeLifecycleState.STARTING.canTransitionTo(NodeLifecycleState.RECOVERING));assertTrue(NodeLifecycleState.RECOVERING.canTransitionTo(NodeLifecycleState.CONTROL_ONLY));}
 @Test void phaseFiveCannotTransitionToDataReady(){for(var s:NodeLifecycleState.values()) assertFalse(s.canTransitionTo(NodeLifecycleState.READY));}
 @Test void fatalFailureCannotBecomeReady(){assertFalse(NodeLifecycleState.FAILED.canTransitionTo(NodeLifecycleState.CONTROL_ONLY));}
 @Test void shutdownIsIdempotent(){assertTrue(NodeLifecycleState.STOPPING.canTransitionTo(NodeLifecycleState.STOPPING));assertTrue(NodeLifecycleState.STOPPED.canTransitionTo(NodeLifecycleState.STOPPED));}
}
