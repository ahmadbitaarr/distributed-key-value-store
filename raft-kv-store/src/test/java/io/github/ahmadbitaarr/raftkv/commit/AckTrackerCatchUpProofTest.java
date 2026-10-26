package io.github.ahmadbitaarr.raftkv.commit;

import io.github.ahmadbitaarr.raftkv.config.ClusterConfigTest;
import io.github.ahmadbitaarr.raftkv.log.*;
import io.github.ahmadbitaarr.raftkv.replication.*;
import io.github.ahmadbitaarr.raftkv.server.NodeRole;
import io.github.ahmadbitaarr.raftkv.wal.FileWalStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import static org.junit.jupiter.api.Assertions.*;

class AckTrackerCatchUpProofTest {
    @TempDir Path dir;
    private static LogEntry e(long index){return new LogEntry(index,"id"+index,Command.put("k",new byte[]{(byte)index}),new byte[]{(byte)index});}
    private static FollowerStatusProbe.Status status(String id,long last,long committed){
        return new FollowerStatusProbe.Status(id,NodeRole.FOLLOWER,last,committed,committed,false);
    }
    @Test void nonemptyStatusEstablishesOnlyPrefixZero()throws Exception{
        var cfg=ClusterConfigTest.config(dir,"n1");
        try(var wal=FileWalStore.open(cfg.walFile());var log=new WalBackedPersistentLog(wal)){
            log.append(e(1));log.append(e(2));
            var tracker=new AckTracker(cfg,log,wal);
            assertTrue(tracker.beginNonemptyVerification("n2",status("n2",2,1)));
            assertEquals(0,tracker.verifiedMatchingThrough("n2"));
            assertTrue(tracker.majorityFor(e(2)).isEmpty());
            assertFalse(tracker.recordDurableAck("n2",e(2),FollowerClient.ack(2,2)));
        }
    }
    @Test void matchingIndexOneRequiresValidatedNonemptyBootstrapBeforeItCanVote() throws Exception {
        var cfg=ClusterConfigTest.config(dir,"n1");
        try(var wal=FileWalStore.open(cfg.walFile());var log=new WalBackedPersistentLog(wal)) {
            var first=e(1);
            log.append(first);
            var tracker=new AckTracker(cfg,log,wal);
            // The same-index ACK by itself is not a certificate.
            assertFalse(tracker.recordDurableAck("n2",first,FollowerClient.ack(1,1)));
            assertTrue(tracker.majorityFor(first).isEmpty());
            // Phase 9's validated nonempty-status bootstrap proves only prefix zero;
            // an exact durable ACK for index 1 then completes the entire prefix [1..1].
            assertTrue(tracker.beginNonemptyVerification("n2",status("n2",1,0)));
            assertEquals(0,tracker.verifiedMatchingThrough("n2"));
            assertTrue(tracker.majorityFor(first).isEmpty());
            assertTrue(tracker.recordDurableAck("n2",first,FollowerClient.ack(1,1)));
            assertTrue(tracker.majorityFor(first).isPresent());
        }
    }
    @Test void exactSequentialDuplicateAcksAdvanceAndIssueCertificate()throws Exception{
        var cfg=ClusterConfigTest.config(dir,"n1");
        try(var wal=FileWalStore.open(cfg.walFile());var log=new WalBackedPersistentLog(wal)){
            log.append(e(1));log.append(e(2));
            var tracker=new AckTracker(cfg,log,wal);
            assertTrue(tracker.beginNonemptyVerification("n2",status("n2",2,0)));
            assertTrue(tracker.recordDurableAck("n2",e(1),FollowerClient.ack(1,2)));
            assertTrue(tracker.recordDurableAck("n2",e(2),FollowerClient.ack(2,2)));
            var certificate=tracker.majorityFor(e(2)).orElseThrow();
            assertTrue(tracker.validates(certificate,e(2)));
            assertEquals(2,certificate.voters().size());
        }
    }
    @Test void wrongRoleOrIdentityCannotStartNonemptyProof()throws Exception{
        var cfg=ClusterConfigTest.config(dir,"n1");
        try(var wal=FileWalStore.open(cfg.walFile());var log=new WalBackedPersistentLog(wal)){
            var tracker=new AckTracker(cfg,log,wal);
            assertFalse(tracker.beginNonemptyVerification("n2",status("n3",4,2)));
            assertFalse(tracker.beginNonemptyVerification("n2",new FollowerStatusProbe.Status("n2",NodeRole.LEADER,4,2,0,false)));
            assertEquals(-1,tracker.verifiedMatchingThrough("n2"));
        }
    }
    @Test void invalidationRevokesAlreadyIssuedProof()throws Exception{
        var cfg=ClusterConfigTest.config(dir,"n1");
        try(var wal=FileWalStore.open(cfg.walFile());var log=new WalBackedPersistentLog(wal)){
            log.append(e(1));var tracker=new AckTracker(cfg,log,wal);
            tracker.certifyEmpty("n2",status("n2",0,0));
            assertTrue(tracker.recordDurableAck("n2",e(1),FollowerClient.ack(1,1)));
            var issued=tracker.majorityFor(e(1)).orElseThrow();
            tracker.invalidate("n2");
            assertFalse(tracker.validates(issued,e(1)));
            assertEquals(-1,tracker.verifiedMatchingThrough("n2"));
            assertTrue(tracker.majorityFor(e(1)).isEmpty());
        }
    }
    @Test void wrongEntryOrAckOrGapCannotCreateProof()throws Exception{
        var cfg=ClusterConfigTest.config(dir,"n1");
        try(var wal=FileWalStore.open(cfg.walFile());var log=new WalBackedPersistentLog(wal)){
            log.append(e(1));log.append(e(2));var tracker=new AckTracker(cfg,log,wal);
            tracker.certifyEmpty("n2",status("n2",0,0));
            assertFalse(tracker.recordDurableAck("n2",e(2),FollowerClient.ack(2,2)));
            assertFalse(tracker.recordDurableAck("n2",new LogEntry(1,"different",e(1).command(),e(1).commandFingerprint()),FollowerClient.ack(1,1)));
            assertFalse(tracker.recordDurableAck("n2",e(1),new FollowerClient.Outcome(FollowerClient.Kind.UNKNOWN_TIMEOUT,1,0,0,"late")));
            assertEquals(0,tracker.verifiedMatchingThrough("n2"));
        }
    }
    @Test void rejectedAndTimedOutResultsCannotRetroactivelyCommit()throws Exception{
        var cfg=ClusterConfigTest.config(dir,"n1");
        try(var wal=FileWalStore.open(cfg.walFile());var log=new WalBackedPersistentLog(wal)){
            log.append(e(1));var tracker=new AckTracker(cfg,log,wal);
            tracker.certifyEmpty("n2",status("n2",0,0));
            var unknown=new FollowerClient.Outcome(FollowerClient.Kind.UNKNOWN_TIMEOUT,1,0,0,"deadline expired");
            assertFalse(tracker.recordDurableAck("n2",e(1),unknown));
            assertTrue(tracker.majorityFor(e(1)).isEmpty());
            assertEquals(0,wal.durableCommitIndex());
        }
    }
}
