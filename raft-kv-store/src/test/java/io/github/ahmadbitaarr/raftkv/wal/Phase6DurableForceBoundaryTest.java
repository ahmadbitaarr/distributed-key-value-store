package io.github.ahmadbitaarr.raftkv.wal;

import io.github.ahmadbitaarr.raftkv.config.ClusterConfigTest;
import io.github.ahmadbitaarr.raftkv.log.Command;
import io.github.ahmadbitaarr.raftkv.log.LogEntry;
import io.github.ahmadbitaarr.raftkv.log.WalBackedPersistentLog;
import io.github.ahmadbitaarr.raftkv.replication.*;
import io.github.ahmadbitaarr.raftkv.server.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import static org.junit.jupiter.api.Assertions.*;

/** Tests the actual FileWalStore channel.force callback rather than merely blocking a wrapper. */
class Phase6DurableForceBoundaryTest {
    @TempDir Path dir;
    private static LogEntry entry(){return new LogEntry(1,"rid",Command.put("x",new byte[]{0,-1}),new byte[]{2});}
    private static final class Rig implements AutoCloseable {
        final FileWalStore store;final WalBackedPersistentLog log;
        final ReplicationOperationGate gate=new ReplicationOperationGate();
        final AtomicReference<NodeStatusSnapshot> state=new AtomicReference<>(new NodeStatusSnapshot("n2",NodeRole.FOLLOWER,NodeLifecycleState.CONTROL_ONLY,0,0,0,false));
        final FollowerReplicationHandler handler;
        Rig(FileWalStore store, Path walDirectory){
            this.store=store;this.log=new WalBackedPersistentLog(store);
            this.handler=new FollowerReplicationHandler(ClusterConfigTest.config(walDirectory,"n2"),log,store,gate,state::get,
                    n->state.updateAndGet(s->new NodeStatusSnapshot("n2",NodeRole.FOLLOWER,s.state(),n,0,0,false)),
                    error->state.updateAndGet(s->s.withState(NodeLifecycleState.FAILED)));
            gate.open();
        }
        public void close()throws IOException{gate.closeAdmission();log.close();}
    }
    @Test void ackWaitsForActualFileChannelForceCompletion()throws Exception {
        CountDownLatch insideForce=new CountDownLatch(1),releaseForce=new CountDownLatch(1);
        var store=FileWalStore.openForTest(dir.resolve("wal"),channel->{
            insideForce.countDown();
            try{if(!releaseForce.await(5,TimeUnit.SECONDS))throw new IOException("test timeout");}
            catch(InterruptedException e){Thread.currentThread().interrupt();throw new IOException(e);}
            channel.force(true);
        },FileWalStore.productionAtomicMover());
        try(Rig rig=new Rig(store,dir)){
            ExecutorService thread=Executors.newSingleThreadExecutor();
            try {
                Future<FollowerReplicationHandler.Decision> result=thread.submit(()->rig.handler.replicate("n1",entry()));
                assertTrue(insideForce.await(2,TimeUnit.SECONDS));
                assertEquals(0,rig.state.get().lastLogIndex());
                assertFalse(result.isDone());
                releaseForce.countDown();
                assertTrue(result.get(5,TimeUnit.SECONDS).accepted());
                assertEquals(1,rig.state.get().lastLogIndex());
            }finally{releaseForce.countDown();thread.shutdownNow();}
        }
    }
    @Test void failedRealForceNeverYieldsAckOrPublishesIndex()throws Exception {
        var store=FileWalStore.openForTest(dir.resolve("wal"),channel->{throw new IOException("injected force failure");},FileWalStore.productionAtomicMover());
        try(Rig rig=new Rig(store,dir)){
            assertThrows(IOException.class,()->rig.handler.replicate("n1",entry()));
            assertEquals(0,rig.state.get().lastLogIndex());
            assertEquals(NodeLifecycleState.FAILED,rig.state.get().state());
            assertEquals(0,rig.gate.activeCount());
            assertThrows(FollowerReplicationHandler.StorageFailedException.class,()->rig.handler.replicate("n1",entry()));
        }
    }
}
