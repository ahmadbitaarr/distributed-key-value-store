package io.github.ahmadbitaarr.raftkv.server;

import com.google.protobuf.ByteString;
import io.grpc.*;
import io.github.ahmadbitaarr.raftkv.config.*;
import io.github.ahmadbitaarr.raftkv.leader.CanonicalCommandFingerprint;
import io.github.ahmadbitaarr.raftkv.rpc.ProtoMapper;
import io.github.ahmadbitaarr.raftkv.rpc.generated.replication.v1.ReplicateEntryRequest;
import io.github.ahmadbitaarr.raftkv.rpc.generated.replication.v1.ReplicationServiceGrpc;
import io.github.ahmadbitaarr.raftkv.log.*;
import io.github.ahmadbitaarr.raftkv.replication.GrpcFollowerClientTestSupport;
import io.github.ahmadbitaarr.raftkv.rpc.generated.client.v1.*;
import io.github.ahmadbitaarr.raftkv.wal.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;

/** Actual Netty communication; each server binds port zero and owns its own WAL. */
class Phase7RealThreeNodeTest {
    @TempDir Path directory;

    private static Map<String,NodeEndpoint> advertisedMembers() {
        // ClusterConfig remains strict. These advertised fixture ports are never bound or used
        // for inter-node traffic: the test-only outbound resolver supplies actual Netty ports.
        return ClusterConfigTest.members();
    }
    private ClusterConfig config(String id) {
        var members=advertisedMembers();
        return new ClusterConfig("phase7",id,"n1",members,"127.0.0.1",members.get(id).port(),
                directory.resolve(id),4000,1500,1000,2000,12582912);
    }
    private static ManagedChannel channel(NodeServer node) {
        assertTrue(node.boundPort()>0,"Netty must be listening on an actual ephemeral port");
        return ManagedChannelBuilder.forAddress("127.0.0.1",node.boundPort()).usePlaintext().build();
    }
    private static void register(Map<String,NodeEndpoint> live, String id, NodeServer node) {
        live.put(id,new NodeEndpoint("127.0.0.1",node.boundPort()));
    }
    private static NodeServer follower(ClusterConfig config) { return new NodeServer(config,0); }
    private static NodeServer leader(ClusterConfig config, Map<String,NodeEndpoint> live) {
        return new NodeServer(config,0,c -> GrpcFollowerClientTestSupport.withEndpointResolver(c,id -> {
            var endpoint=live.get(id);
            if(endpoint==null)throw new IllegalArgumentException("test follower not running: "+id);
            return endpoint;
        }));
    }
    private static MutationResponse put(ManagedChannel channel,String id,String key,byte[] bytes) {
        return KeyValueServiceGrpc.newBlockingStub(channel).withDeadlineAfter(8,TimeUnit.SECONDS)
                .put(PutRequest.newBuilder().setRequestId(id).setKey(key)
                        .setValue(ByteString.copyFrom(bytes)).build());
    }
    private static MutationResponse delete(ManagedChannel channel,String id,String key) {
        return KeyValueServiceGrpc.newBlockingStub(channel).withDeadlineAfter(8,TimeUnit.SECONDS)
                .delete(DeleteRequest.newBuilder().setRequestId(id).setKey(key).build());
    }

    @Test void realHealthyThreeNodePutDeleteAndOrderedApplication() throws Exception {
        var c1=config("n1");var c2=config("n2");var c3=config("n3");
        var live=new java.util.concurrent.ConcurrentHashMap<String,NodeEndpoint>();
        try(var n2=follower(c2);var n3=follower(c3);var leader=leader(c1,live)) {
            n2.start();register(live,"n2",n2);
            n3.start();register(live,"n3",n3);
            leader.start();
            var ch=channel(leader);
            try {
                var created=put(ch,"p1","key",new byte[0]);
                assertEquals(ResultCode.RESULT_CODE_OK,created.getCode());
                assertEquals(1,created.getCommitIndex());
                assertEquals(1,leader.statusSnapshot().durableCommitIndex());
                assertEquals(1,leader.statusSnapshot().appliedIndex());
                // Phase 9 asynchronously propagates follower COMMIT and application.
                long followerDeadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(8);
                while ((n2.statusSnapshot().appliedIndex()!=1 || n3.statusSnapshot().appliedIndex()!=1)
                        && System.nanoTime()<followerDeadline) Thread.sleep(20);
                assertEquals(1,n2.statusSnapshot().durableCommitIndex());
                assertEquals(1,n2.statusSnapshot().appliedIndex());
                assertEquals(1,n3.statusSnapshot().appliedIndex());
                var deleted=delete(ch,"d2","key");
                assertEquals(ResultCode.RESULT_CODE_OK,deleted.getCode());
                assertEquals(2,deleted.getCommitIndex());
                assertEquals(2,leader.statusSnapshot().lastLogIndex());
                assertEquals(2,leader.statusSnapshot().durableCommitIndex());
                assertEquals(2,leader.statusSnapshot().appliedIndex());
                var read=KeyValueServiceGrpc.newBlockingStub(ch)
                        .get(GetRequest.newBuilder().setKey("key").build());
                assertEquals(ResultCode.RESULT_CODE_NOT_FOUND,read.getCode());
                assertEquals(2,read.getCommitIndex());
            } finally {ch.shutdownNow();}
        }
        try(var recovered=FileWalStore.open(c1.walFile())) {
            assertEquals(2,recovered.lastDurableLogIndex());
            assertEquals(2,recovered.durableCommitIndex());
        }
    }

    @Test void oneHealthyFollowerCanCommitWithoutTheOther() throws Exception {
        var c1=config("n1");var c2=config("n2");var live=new java.util.concurrent.ConcurrentHashMap<String,NodeEndpoint>();
        try(var n2=follower(c2);var leader=leader(c1,live)) {
            n2.start();register(live,"n2",n2);leader.start();
            var ch=channel(leader);
            try {
                var response=put(ch,"available","a",new byte[]{1,2});
                assertEquals(ResultCode.RESULT_CODE_OK,response.getCode());
                assertEquals(1,leader.statusSnapshot().durableCommitIndex());
                assertEquals(1,n2.statusSnapshot().lastLogIndex());
                long followerDeadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(8);
                while(n2.statusSnapshot().durableCommitIndex()!=1 && System.nanoTime()<followerDeadline)
                    Thread.sleep(20);
                assertEquals(1,n2.statusSnapshot().durableCommitIndex());
                assertEquals(1,n2.statusSnapshot().appliedIndex());
            } finally {ch.shutdownNow();}
        }
    }

    @Test void durableSecondEntryCannotRegressAppliedIndexAfterFirstCommit() throws Exception {
        var c1=config("n1");var c2=config("n2");var live=new java.util.concurrent.ConcurrentHashMap<String,NodeEndpoint>();
        try(var n2=follower(c2);var leader=leader(c1,live)) {
            n2.start();register(live,"n2",n2);leader.start();
            var ch=channel(leader);
            try {
                assertEquals(ResultCode.RESULT_CODE_OK,
                        put(ch,"first-committed","key",new byte[]{1}).getCode());
                var first=leader.statusSnapshot();
                assertEquals(1,first.lastLogIndex());
                assertEquals(1,first.durableCommitIndex());
                assertEquals(1,first.appliedIndex());

                // Remove the only available follower. The next ENTRY is forced locally,
                // but cannot obtain a matching-prefix majority or become committed.
                n2.close();
                live.remove("n2");
                assertEquals(ResultCode.RESULT_CODE_NO_QUORUM,
                        put(ch,"second-uncommitted","key",new byte[]{2}).getCode());

                // This snapshot is after the second durable ENTRY publication but
                // before any COMMIT(2): applied progress must remain at index 1.
                var afterNoQuorum=leader.statusSnapshot();
                assertEquals(2,afterNoQuorum.lastLogIndex());
                assertEquals(1,afterNoQuorum.durableCommitIndex());
                assertEquals(1,afterNoQuorum.appliedIndex(),
                        "a new durable ENTRY must not reset already applied progress");
                assertEquals(ResultCode.RESULT_CODE_NOT_READY,
                        put(ch,"third-blocked","another",new byte[]{3}).getCode());
                assertEquals(1,leader.statusSnapshot().appliedIndex());
            } finally {ch.shutdownNow();}
        }
        try(var recovered=FileWalStore.open(c1.walFile())) {
            assertEquals(2,recovered.lastDurableLogIndex(),
                    "second ENTRY must really be durable, not just reported");
            assertEquals(1,recovered.durableCommitIndex(),
                    "NO_QUORUM must not force COMMIT(2)");
        }
    }

    @Test void neitherFollowerAvailableReturnsNoQuorumAndFreezesFutureWrites() throws Exception {
        var live=new java.util.concurrent.ConcurrentHashMap<String,NodeEndpoint>();
        try(var leader=leader(config("n1"),live)) {
            leader.start();var ch=channel(leader);
            try {
                assertEquals(ResultCode.RESULT_CODE_NO_QUORUM,put(ch,"original","x",new byte[]{1}).getCode());
                assertEquals(ResultCode.RESULT_CODE_NOT_READY,put(ch,"later","y",new byte[]{2}).getCode());
                assertEquals(ResultCode.RESULT_CODE_NOT_READY,put(ch,"original","x",new byte[]{1}).getCode());
                assertEquals(1,leader.statusSnapshot().lastLogIndex());
                assertEquals(0,leader.statusSnapshot().durableCommitIndex());
                assertEquals(0,leader.statusSnapshot().appliedIndex());
            } finally {ch.shutdownNow();}
        }
    }

    @Test void sameIndexAckOnUnverifiedHistoryIsNotProofOfMatchingPrefix() throws Exception {
        // Phase 9 is allowed to verify a nonempty follower by replaying exact entries.
        // At index 1 a validated prefix-zero start plus an exact duplicate ACK is
        // sufficient proof. The safety case must therefore use index 2 with an
        // actually divergent, COMMITTED index 1 that can never be reconciled.
        var c1=config("n1");var c2=config("n2");var c3=config("n3");
        var live=new java.util.concurrent.ConcurrentHashMap<String,NodeEndpoint>();
        var divergentCommand=Command.put("different-history",new byte[]{9});
        var divergentFirst=new LogEntry(1,"other-history",divergentCommand,
                CanonicalCommandFingerprint.of(divergentCommand));
        var secondCommand=Command.put("x",new byte[]{1});
        var matchingSecond=new LogEntry(2,"same-id",secondCommand,
                CanonicalCommandFingerprint.of(secondCommand));
        try(var seeded=FileWalStore.open(c2.walFile())) {
            seeded.appendEntryDurably(divergentFirst);
            seeded.appendEntryDurably(matchingSecond);
            seeded.appendCommitDurably(1); // a committed conflict must never be rewritten
        }
        try(var n2=follower(c2);var n3=follower(c3);var leader=leader(c1,live)) {
            n2.start();register(live,"n2",n2);
            n3.start();register(live,"n3",n3);
            leader.start();var ch=channel(leader);
            try {
                // Commit leader index 1 with n3, not the divergent n2.
                assertEquals(ResultCode.RESULT_CODE_OK,
                        put(ch,"leader-first","correct-history",new byte[]{7}).getCode());
                assertEquals(1,leader.statusSnapshot().durableCommitIndex());
                assertEquals(1,n2.statusSnapshot().durableCommitIndex());

                // Remove the only follower with a verified matching prefix.
                n3.close();live.remove("n3");

                // n2 already holds EXACTLY the future leader entry at index 2.
                // The leader must reject that same-index ACK as quorum evidence
                // because the committed predecessor at index 1 is divergent.
                assertEquals(ResultCode.RESULT_CODE_NO_QUORUM,
                        put(ch,"same-id","x",new byte[]{1}).getCode(),
                        "index-2 duplicate ACK without matching index 1 cannot form a majority");

                // After the leader ENTRY(2) is durable, prove using real gRPC
                // that n2 can positively ACK this exact duplicate at index 2.
                // This late verification must not retroactively create a COMMIT.
                var n2Channel=channel(n2);
                try {
                    var duplicate=ReplicationServiceGrpc.newBlockingStub(n2Channel)
                            .withDeadlineAfter(4,TimeUnit.SECONDS)
                            .replicateEntry(ReplicateEntryRequest.newBuilder()
                                    .setLeaderId("n1")
                                    .setEntry(ProtoMapper.toProtoLogEntry(matchingSecond))
                                    .setLeaderCommitIndex(1).build());
                    assertTrue(duplicate.getAccepted(),"exact index-2 retransmission is a durable ACK");
                    assertEquals("n2",duplicate.getFollowerId());
                    assertEquals(2,duplicate.getLastPersistedIndex());
                } finally {n2Channel.shutdownNow();}

                assertEquals(2,leader.statusSnapshot().lastLogIndex());
                assertEquals(1,leader.statusSnapshot().durableCommitIndex());
                assertEquals(1,leader.statusSnapshot().appliedIndex());
                assertEquals(2,n2.statusSnapshot().lastLogIndex());
                assertEquals(1,n2.statusSnapshot().durableCommitIndex());
                assertEquals(ResultCode.RESULT_CODE_NOT_READY,
                        put(ch,"blocked-third","x",new byte[]{3}).getCode(),
                        "unresolved suffix must remain blocked despite duplicate ACK");
            } finally {ch.shutdownNow();}
        }
        // Confirm that background catch-up never destroyed the incompatible committed prefix.
        try(var leaderWal=FileWalStore.open(c1.walFile());
            var followerWal=FileWalStore.open(c2.walFile())) {
            assertEquals(2,leaderWal.lastDurableLogIndex());
            assertEquals(1,leaderWal.durableCommitIndex());
            assertEquals(2,followerWal.lastDurableLogIndex());
            assertEquals(1,followerWal.durableCommitIndex());
            assertEquals(divergentFirst,followerWal.recoveryResult().entries().get(0));
            assertEquals(matchingSecond,followerWal.recoveryResult().entries().get(1));
        }
    }

    @Test void recoveredCommittedLeaderWithoutFollowerWitnessCannotResumeWriting() throws Exception {
        var c1=config("n1");var c2=config("n2");var live=new java.util.concurrent.ConcurrentHashMap<String,NodeEndpoint>();
        try(var n2=follower(c2);var leader=leader(c1,live)) {
            n2.start();register(live,"n2",n2);leader.start();var ch=channel(leader);
            try {assertEquals(ResultCode.RESULT_CODE_OK,put(ch,"first","x",new byte[]{1}).getCode());}
            finally {ch.shutdownNow();}
        }
        try(var restarted=leader(c1,live)) {
            restarted.start();var ch=channel(restarted);
            try {
                assertEquals(ResultCode.RESULT_CODE_NOT_READY,put(ch,"second","x",new byte[]{2}).getCode());
                assertEquals(1,restarted.statusSnapshot().durableCommitIndex());
                assertEquals(1,restarted.statusSnapshot().appliedIndex()); // Phase 10 local replay, still not READY
            } finally {ch.shutdownNow();}
        }
    }

    @Test void followersAlwaysReturnNotLeaderAndNeverAdvanceCommit() throws Exception {
        var c2=config("n2");
        try(var follower=follower(c2)) {
            follower.start();var ch=channel(follower);
            try {
                assertEquals(ResultCode.RESULT_CODE_NOT_LEADER,put(ch,"id","x",new byte[]{1}).getCode());
                assertEquals(ResultCode.RESULT_CODE_NOT_LEADER,delete(ch,"id2","x").getCode());
                assertEquals(ResultCode.RESULT_CODE_NOT_LEADER,KeyValueServiceGrpc.newBlockingStub(ch)
                        .get(GetRequest.newBuilder().setKey("x").build()).getCode());
                assertEquals(0,follower.statusSnapshot().durableCommitIndex());
            } finally {ch.shutdownNow();}
        }
    }
}
