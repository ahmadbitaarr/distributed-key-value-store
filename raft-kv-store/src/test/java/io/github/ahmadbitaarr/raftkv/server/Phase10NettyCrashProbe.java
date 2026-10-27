package io.github.ahmadbitaarr.raftkv.server;

import io.grpc.ManagedChannelBuilder;
import io.github.ahmadbitaarr.raftkv.commit.AckTracker;
import io.github.ahmadbitaarr.raftkv.config.*;
import io.github.ahmadbitaarr.raftkv.leader.CanonicalCommandFingerprint;
import io.github.ahmadbitaarr.raftkv.log.*;
import io.github.ahmadbitaarr.raftkv.recovery.RecoveryManager;
import io.github.ahmadbitaarr.raftkv.replication.*;
import io.github.ahmadbitaarr.raftkv.rpc.ProtoMapper;
import io.github.ahmadbitaarr.raftkv.rpc.generated.replication.v1.ReplicateEntryRequest;
import io.github.ahmadbitaarr.raftkv.rpc.generated.replication.v1.ReplicationServiceGrpc;
import io.github.ahmadbitaarr.raftkv.statemachine.*;
import io.github.ahmadbitaarr.raftkv.wal.FileWalStore;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/** Real Netty + persistent WAL child fixture; exits without any Java cleanup. */
public final class Phase10NettyCrashProbe {
    private Phase10NettyCrashProbe() { }
    static LogEntry entry(int index) {
        var command=Command.put("key-"+index,new byte[]{(byte)index,0});
        return new LogEntry(index,"crash-netty-"+index,command,CanonicalCommandFingerprint.of(command));
    }
    public static void main(String[] args) throws Exception {
        if(args.length!=2)throw new IllegalArgumentException("mode and directory required");
        Path root=Path.of(args[1]);
        var leaderConfig=ClusterConfigTest.config(root.resolve("n1"),"n1");
        var followerConfig=ClusterConfigTest.config(root.resolve("n2"),"n2");
        // Only one owner per WAL, even in the child process.
        try(var leaderWal=FileWalStore.open(leaderConfig.walFile())) {
            var log=new WalBackedPersistentLog(leaderWal);
            log.append(entry(1));
            if(args[0].equals("RECONCILIATION_AFTER_FIRST_ACK")) {
                log.append(entry(2));leaderWal.appendCommitDurably(2);
            }
            try(var follower=new NodeServer(followerConfig,0)) {
                follower.start();
                if(args[0].equals("FOLLOWER_ACK_BEFORE_LEADER_COMMIT")) {
                    var channel=ManagedChannelBuilder.forAddress("127.0.0.1",follower.boundPort())
                            .usePlaintext().build();
                    var response=ReplicationServiceGrpc.newBlockingStub(channel)
                            .withDeadlineAfter(5,TimeUnit.SECONDS)
                            .replicateEntry(ReplicateEntryRequest.newBuilder()
                                    .setLeaderId("n1").setEntry(ProtoMapper.toProtoLogEntry(entry(1)))
                                    .setLeaderCommitIndex(0).build());
                    if(!response.getAccepted() || follower.statusSnapshot().lastLogIndex()!=1
                            || follower.statusSnapshot().durableCommitIndex()!=0)
                        throw new AssertionError("follower durable ACK boundary not established");
                    Runtime.getRuntime().halt(44); // BEFORE leader COMMIT
                }
                if(!args[0].equals("RECONCILIATION_AFTER_FIRST_ACK"))
                    throw new IllegalArgumentException("unknown mode");
                var actual=GrpcFollowerClientTestSupport.withEndpointResolver(leaderConfig,
                        id -> new NodeEndpoint("127.0.0.1",follower.boundPort()));
                FollowerMaintenanceClient interrupted=new FollowerMaintenanceClient(){
                    @Override public Optional<Status> probeStatus(String id){return actual.probeStatus(id);}
                    @Override public Outcome replicate(String id,LogEntry e){
                        Outcome outcome=actual.replicate(id,e);
                        if(e.index()==1 && outcome.kind()==FollowerClient.Kind.ACK)
                            Runtime.getRuntime().halt(45); // follower WAL forced; no recovery witness
                        return outcome;
                    }
                    @Override public Result advanceCommit(String id,long index){return actual.advanceCommit(id,index);}
                    @Override public Result truncateUncommittedSuffix(String id,long retain){return actual.truncateUncommittedSuffix(id,retain);}
                };
                var applier=new StateMachineApplier(new KvStateMachine());
                RecoveryManager.recoverLocal(log,leaderWal,applier);
                try(var manager=new ReplicationManager(leaderConfig,log,interrupted);
                    var recovery=new RecoveryManager(leaderConfig,log,leaderWal,applier,manager,
                            interrupted,new AckTracker(leaderConfig,log,leaderWal),
                            ignored -> { throw new AssertionError("witness before final reconciliation"); },
                            fault -> {throw new AssertionError("unexpected fatal",fault);})){
                    recovery.runOnce("n2");
                }
                throw new AssertionError("first replica ACK did not halt child");
            }
        }
    }
}
