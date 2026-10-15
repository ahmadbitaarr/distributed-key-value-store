package io.github.ahmadbitaarr.raftkv.server;

import io.grpc.Server;
import io.github.ahmadbitaarr.raftkv.config.ClusterConfigTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;

/** The fake models gRPC termination results, not distributed work or an alternate server. */
class NodeServerShutdownTest {
 @TempDir Path dir;
 private static final class ControlledServer extends Server {
  boolean gracefullyTerminates, forciblyTerminates, forciblyTerminated;
  int gracefulCount, forceCount, awaitCount;
  ControlledServer(boolean graceful, boolean forced){gracefullyTerminates=graceful;forciblyTerminates=forced;}
  @Override public Server start() {return this;}
  @Override public Server shutdown(){gracefulCount++;return this;}
  @Override public Server shutdownNow(){forceCount++;if(forciblyTerminates)forciblyTerminated=true;return this;}
  @Override public boolean isShutdown(){return gracefulCount>0 || forceCount>0;}
  @Override public boolean isTerminated(){return (gracefulCount>0 && gracefullyTerminates)||forciblyTerminated;}
  @Override public boolean awaitTermination(long timeout,TimeUnit unit){awaitCount++;return isTerminated();}
  @Override public void awaitTermination(){}
  @Override public int getPort(){return 50101;}
 }
 private NodeServer node(ControlledServer fake){return new NodeServer(ClusterConfigTest.config(dir,"n1"),0,(cfg,port,client,peers)->fake);}
 @Test void gracefulShutdownTerminatesBeforeWalRelease()throws Exception{
   var fake=new ControlledServer(true,true);var n=node(fake);n.start();n.close();
   assertEquals(1,fake.gracefulCount);assertEquals(0,fake.forceCount);
   assertTrue(fake.awaitCount>=1);assertEquals(NodeLifecycleState.STOPPED,n.statusSnapshot().state());
   try(var lease=WalDirectoryLease.acquire(dir)){assertTrue(lease.isHeld());}
 }
 @Test void forcedShutdownEscalatesAfterBoundedGracePeriod()throws Exception{
   var fake=new ControlledServer(false,true);var n=node(fake);n.start();n.close();
   assertEquals(1,fake.gracefulCount);assertEquals(1,fake.forceCount);assertEquals(2,fake.awaitCount);
   assertEquals(NodeLifecycleState.STOPPED,n.statusSnapshot().state());
 }
 @Test void shutdownTimeoutKeepsWalLeaseAndNeverClaimsStopped()throws Exception{
   var fake=new ControlledServer(false,false);var n=node(fake);n.start();
   assertThrows(IOException.class,n::close);
   assertEquals(NodeLifecycleState.STOPPING,n.statusSnapshot().state());
   assertEquals(1,fake.forceCount);
   assertThrows(IOException.class,()->WalDirectoryLease.acquire(dir));
   fake.forciblyTerminates=true;n.close();
   assertEquals(NodeLifecycleState.STOPPED,n.statusSnapshot().state());
   try(var lease=WalDirectoryLease.acquire(dir)){assertTrue(lease.isHeld());}
 }
}
