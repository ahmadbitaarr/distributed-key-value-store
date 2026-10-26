package io.github.ahmadbitaarr.raftkv.server;

import com.google.protobuf.ByteString;
import io.grpc.*;
import io.github.ahmadbitaarr.raftkv.config.*;
import io.github.ahmadbitaarr.raftkv.leader.CanonicalCommandFingerprint;
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
        var live=new HashMap<String,NodeEndpoint>();
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
                assertEquals(0,n2.statusSnapshot().durableCommitIndex());
                assertEquals(0,n3.statusSnapshot().appliedIndex());
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
        var c1=config("n1");var c2=config("n2");var live=new HashMap<String,NodeEndpoint>();
        try(var n2=follower(c2);var leader=leader(c1,live)) {
            n2.start();register(live,"n2",n2);leader.start();
            var ch=channel(leader);
            try {
                var response=put(ch,"available","a",new byte[]{1,2});
                assertEquals(ResultCode.RESULT_CODE_OK,response.getCode());
                assertEquals(1,leader.statusSnapshot().durableCommitIndex());
                assertEquals(1,n2.statusSnapshot().lastLogIndex());
                assertEquals(0,n2.statusSnapshot().durableCommitIndex());
            } finally {ch.shutdownNow();}
        }
    }

    @Test void durableSecondEntryCannotRegressAppliedIndexAfterFirstCommit() throws Exception {
        var c1=config("n1");var c2=config("n2");var live=new HashMap<String,NodeEndpoint>();
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
        var live=new HashMap<String,NodeEndpoint>();
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
        var c1=config("n1");var c2=config("n2");var live=new HashMap<String,NodeEndpoint>();
        var command=Command.put("x",new byte[]{1});
        var prior=new LogEntry(1,"same-id",command,CanonicalCommandFingerprint.of(command));
        try(var seeded=FileWalStore.open(c2.walFile())){seeded.appendEntryDurably(prior);}
        try(var n2=follower(c2);var leader=leader(c1,live)) {
            n2.start();register(live,"n2",n2);leader.start();var ch=channel(leader);
            try {
                assertEquals(ResultCode.RESULT_CODE_NO_QUORUM,put(ch,"same-id","x",new byte[]{1}).getCode());
                assertEquals(0,leader.statusSnapshot().durableCommitIndex());
                assertEquals(1,n2.statusSnapshot().lastLogIndex());
            } finally {ch.shutdownNow();}
        }
    }

    @Test void recoveredCommittedLeaderCannotResumeWritingWithoutPhase10() throws Exception {
        var c1=config("n1");var c2=config("n2");var live=new HashMap<String,NodeEndpoint>();
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
                assertEquals(0,restarted.statusSnapshot().appliedIndex());
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
