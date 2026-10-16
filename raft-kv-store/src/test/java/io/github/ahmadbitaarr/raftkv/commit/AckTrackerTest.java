package io.github.ahmadbitaarr.raftkv.commit;

import io.github.ahmadbitaarr.raftkv.config.ClusterConfigTest;
import io.github.ahmadbitaarr.raftkv.log.*;
import io.github.ahmadbitaarr.raftkv.replication.*;
import io.github.ahmadbitaarr.raftkv.server.NodeRole;
import io.github.ahmadbitaarr.raftkv.wal.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import static org.junit.jupiter.api.Assertions.*;

class AckTrackerTest {
    @TempDir Path dir;
    private FileWalStore wal;
    private WalBackedPersistentLog log;
    private AckTracker tracker;
    @BeforeEach void setup() throws Exception {
        var cfg=ClusterConfigTest.config(dir,"n1");
        wal=FileWalStore.open(cfg.walFile());log=new WalBackedPersistentLog(wal);
        tracker=new AckTracker(cfg,log,wal);
    }
    @AfterEach void stop() throws Exception {log.close();}
    private LogEntry entry(long n){return new LogEntry(n,"request-"+n,Command.put("a",new byte[]{(byte)n}),new byte[]{(byte)n});}
    private FollowerStatusProbe.Status status(String id,long last,long commit){return new FollowerStatusProbe.Status(id,NodeRole.FOLLOWER,last,commit,0,false);}
    private void empty(){assertTrue(tracker.certifyEmpty("n2",status("n2",0,0)));}

    @Test void freshEmptyFollowerCanBootstrapMatchingPrefixZero(){empty();assertEquals(0,tracker.verifiedMatchingThrough("n2"));}
    @Test void unknownNonemptyFollowerCannotBootstrap(){assertFalse(tracker.certifyEmpty("n2",status("n2",1,0)));assertEquals(-1,tracker.verifiedMatchingThrough("n2"));}
    @Test void wrongFollowerIdentityCannotBootstrap(){assertFalse(tracker.certifyEmpty("n2",status("n3",0,0)));assertEquals(-1,tracker.verifiedMatchingThrough("n2"));}
    @Test void wrongRoleCannotBootstrap(){assertFalse(tracker.certifyEmpty("n2",new FollowerStatusProbe.Status("n2",NodeRole.LEADER,0,0,0,false)));}
    @Test void noLeaderDurabilityCannotProduceCertificate(){empty();assertFalse(tracker.recordDurableAck("n2",entry(1),FollowerClient.ack(1,1)));assertTrue(tracker.majorityFor(entry(1)).isEmpty());}
    @Test void firstExactDurableAckCertifiesLeaderAndOneFollower() throws Exception {
        empty();var e=entry(1);log.append(e);
        assertTrue(tracker.recordDurableAck("n2",e,FollowerClient.ack(1,1)));
        var certificate=tracker.majorityFor(e).orElseThrow();
        assertEquals(2,certificate.voters().size());assertTrue(certificate.voters().contains("n1"));
        assertTrue(tracker.validates(certificate,e));
    }
    @Test void twoFollowersWithoutLocalDurableEntryDoNotCount(){
        tracker.certifyEmpty("n2",status("n2",0,0));tracker.certifyEmpty("n3",status("n3",0,0));
        assertFalse(tracker.recordDurableAck("n2",entry(1),FollowerClient.ack(1,1)));
        assertFalse(tracker.recordDurableAck("n3",entry(1),FollowerClient.ack(1,1)));
        assertTrue(tracker.majorityFor(entry(1)).isEmpty());
    }
    @Test void laterAckWithoutVerifiedPriorPrefixCannotCount() throws Exception {
        empty();log.append(entry(1));log.append(entry(2));
        assertFalse(tracker.recordDurableAck("n2",entry(2),FollowerClient.ack(2,2)));
        assertTrue(tracker.majorityFor(entry(2)).isEmpty());
    }
    @Test void diagnosticHighWaterMarkCannotSkipIndexes() throws Exception {
        empty();log.append(entry(1));log.append(entry(2));
        assertTrue(tracker.recordDurableAck("n2",entry(1),FollowerClient.ack(1,500)));
        assertEquals(1,tracker.verifiedMatchingThrough("n2"));
        assertTrue(tracker.majorityFor(entry(2)).isEmpty());
    }
    @Test void contiguousAcksAdvanceMatchingPrefix() throws Exception {
        empty();log.append(entry(1));log.append(entry(2));
        tracker.recordDurableAck("n2",entry(1),FollowerClient.ack(1,1));
        assertTrue(tracker.recordDurableAck("n2",entry(2),FollowerClient.ack(2,2)));
        assertTrue(tracker.majorityFor(entry(2)).isPresent());
    }
    @Test void wrongIndexWrongEntryAndTimeoutCannotCount() throws Exception {
        empty();log.append(entry(1));
        assertFalse(tracker.recordDurableAck("n2",entry(1),FollowerClient.ack(2,2)));
        assertFalse(tracker.recordDurableAck("n2",new LogEntry(1,"other",Command.delete("a"),new byte[]{1}),FollowerClient.ack(1,1)));
        assertFalse(tracker.recordDurableAck("n2",entry(1),new FollowerClient.Outcome(FollowerClient.Kind.UNKNOWN_TIMEOUT,1,1,0,"late")));
    }
    @Test void duplicateFollowerVoteDoesNotDoubleCount() throws Exception {
        empty();log.append(entry(1));tracker.recordDurableAck("n2",entry(1),FollowerClient.ack(1,1));
        assertFalse(tracker.recordDurableAck("n2",entry(1),FollowerClient.ack(1,1)));
        assertEquals(2,tracker.majorityFor(entry(1)).orElseThrow().voters().size());
    }
    @Test void commitPersistsOnlyWithIssuedCertificate() throws Exception {
        empty();var e=entry(1);log.append(e);
        var cm=new CommitManager(tracker,wal,log,index->{});
        assertThrows(IllegalArgumentException.class,()->cm.commit(e,null));
        tracker.recordDurableAck("n2",e,FollowerClient.ack(1,1));
        cm.commit(e,tracker.majorityFor(e).orElseThrow());
        assertEquals(1,wal.durableCommitIndex());
        assertThrows(IllegalStateException.class,()->cm.commit(e,tracker.majorityFor(e).orElseThrow()));
    }
}
