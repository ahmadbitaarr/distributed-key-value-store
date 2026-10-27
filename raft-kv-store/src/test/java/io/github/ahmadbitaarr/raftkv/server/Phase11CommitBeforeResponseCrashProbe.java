package io.github.ahmadbitaarr.raftkv.server;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannelBuilder;
import io.github.ahmadbitaarr.raftkv.config.*;
import io.github.ahmadbitaarr.raftkv.log.LogEntry;
import io.github.ahmadbitaarr.raftkv.replication.GrpcFollowerClientTestSupport;
import io.github.ahmadbitaarr.raftkv.rpc.generated.client.v1.*;
import io.github.ahmadbitaarr.raftkv.wal.*;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** A real RPC must reach leader COMMIT force after an actual follower ACK, then halt before client response. */
public final class Phase11CommitBeforeResponseCrashProbe {
    private Phase11CommitBeforeResponseCrashProbe() { }
    private static final class HaltAfterCommit implements WalStore {
        final FileWalStore durable;
        HaltAfterCommit(Path path)throws IOException{durable=FileWalStore.open(path);}
        @Override public WalRecoveryResult recoveryResult(){return durable.recoveryResult();}
        @Override public long lastDurableLogIndex(){return durable.lastDurableLogIndex();}
        @Override public long durableCommitIndex(){return durable.durableCommitIndex();}
        @Override public void appendEntryDurably(LogEntry entry)throws IOException{durable.appendEntryDurably(entry);}
        @Override public void appendCommitDurably(long index)throws IOException {
            durable.appendCommitDurably(index); // real file force is complete
            if(index==1)Runtime.getRuntime().halt(57); // observer has not been called
        }
        @Override public void rewriteDurably(List<LogEntry> entries,long index)throws IOException{durable.rewriteDurably(entries,index);}
        @Override public void close()throws IOException{durable.close();}
    }
    public static void main(String[] args)throws Exception {
        if(args.length!=1)throw new IllegalArgumentException("WAL root required");
        Path root=Path.of(args[0]);
        var cfg2=ClusterConfigTest.config(root.resolve("n2"),"n2");
        var cfg1=ClusterConfigTest.config(root.resolve("n1"),"n1");
        try(var follower=new NodeServer(cfg2,0)) {
            follower.start();
            var leader=new NodeServer(cfg1,0,HaltAfterCommit::new,c ->
                    GrpcFollowerClientTestSupport.withEndpointResolver(c,id->{
                        if(!id.equals("n2"))throw new IllegalArgumentException("offline follower");
                        return new NodeEndpoint("127.0.0.1",follower.boundPort());
                    }));
            leader.start();
            var channel=ManagedChannelBuilder.forAddress("127.0.0.1",leader.boundPort()).usePlaintext().build();
            // This call can never receive a success response: COMMIT force halts the JVM first.
            KeyValueServiceGrpc.newBlockingStub(channel).withDeadlineAfter(8,TimeUnit.SECONDS)
                    .put(PutRequest.newBuilder().setRequestId("hard-crash-fixed-id")
                            .setKey("hard-crash-key").setValue(ByteString.copyFrom(new byte[]{0,9})).build());
            throw new AssertionError("COMMIT boundary was not reached");
        }
    }
}
