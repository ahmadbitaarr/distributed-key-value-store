package io.github.ahmadbitaarr.raftkv.server;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
class NodeLifecycleStateTest {
 @Test void followsAllowedStartupTransitions(){assertTrue(NodeLifecycleState.NEW.canTransitionTo(NodeLifecycleState.STARTING));assertTrue(NodeLifecycleState.STARTING.canTransitionTo(NodeLifecycleState.RECOVERING));assertTrue(NodeLifecycleState.RECOVERING.canTransitionTo(NodeLifecycleState.CONTROL_ONLY));}
 @Test void onlyControlOnlyCanTransitionToReady(){
  assertTrue(NodeLifecycleState.CONTROL_ONLY.canTransitionTo(NodeLifecycleState.READY));
  for(var s:NodeLifecycleState.values())if(s!=NodeLifecycleState.CONTROL_ONLY)
   assertFalse(s.canTransitionTo(NodeLifecycleState.READY));
  assertTrue(NodeLifecycleState.READY.canTransitionTo(NodeLifecycleState.FAILED));
  assertTrue(NodeLifecycleState.READY.canTransitionTo(NodeLifecycleState.STOPPING));
 }
 @Test void fatalFailureCannotBecomeReady(){assertFalse(NodeLifecycleState.FAILED.canTransitionTo(NodeLifecycleState.CONTROL_ONLY));}
 @Test void shutdownIsIdempotent(){assertTrue(NodeLifecycleState.STOPPING.canTransitionTo(NodeLifecycleState.STOPPING));assertTrue(NodeLifecycleState.STOPPED.canTransitionTo(NodeLifecycleState.STOPPED));}
}
