package io.github.ahmadbitaarr.raftkv.server;

import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.github.ahmadbitaarr.raftkv.config.ClusterConfig;
import java.util.function.Function;
import io.github.ahmadbitaarr.raftkv.log.WalBackedPersistentLog;
import io.github.ahmadbitaarr.raftkv.replication.FollowerReplicationHandler;
import io.github.ahmadbitaarr.raftkv.replication.ReplicationOperationGate;
import io.github.ahmadbitaarr.raftkv.replication.GrpcFollowerClient;
import io.github.ahmadbitaarr.raftkv.replication.ReplicationManager;
import io.github.ahmadbitaarr.raftkv.commit.AckTracker;
import io.github.ahmadbitaarr.raftkv.commit.CommitManager;
import io.github.ahmadbitaarr.raftkv.leader.LeaderCoordinator;
import io.github.ahmadbitaarr.raftkv.statemachine.KvStateMachine;
import io.github.ahmadbitaarr.raftkv.statemachine.StateMachineApplier;
import io.github.ahmadbitaarr.raftkv.rpc.*;
import io.github.ahmadbitaarr.raftkv.wal.*;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/** Single WAL owner; CONTROL_ONLY permits follower replication, never client data readiness. */
public final class NodeServer implements AutoCloseable {
    @FunctionalInterface interface ServerFactory {
        Server build(ClusterConfig config,int port,ClientKvGrpcService client,ReplicationGrpcService replication);
    }
    @FunctionalInterface interface WalOpener { WalStore open(java.nio.file.Path path) throws IOException; }

    private final ClusterConfig config;
    private final int port;
    private final ServerFactory serverFactory;
    private final WalOpener walOpener;
    private final Function<ClusterConfig,GrpcFollowerClient> outboundClientFactory;
    private final ReplicationOperationGate replicationGate=new ReplicationOperationGate();
    private final AtomicReference<NodeStatusSnapshot> published;
    private final AtomicReference<IOException> storageFault=new AtomicReference<>();
    private WalDirectoryLease lease;
    private WalBackedPersistentLog log;
    private Server server;
    private ReplicationManager replicationManager;
    private LeaderCoordinator leaderCoordinator;
    private StateMachineApplier leaderApplier;

    public NodeServer(ClusterConfig config){this(config,-1,NodeServer::buildNetty,FileWalStore::open);}
    NodeServer(ClusterConfig config,int testPortOverride){this(config,testPortOverride,NodeServer::buildNetty,FileWalStore::open);}
    NodeServer(ClusterConfig config,int testPortOverride,ServerFactory serverFactory){
        this(config,testPortOverride,serverFactory,FileWalStore::open);
    }
    NodeServer(ClusterConfig config,int testPortOverride,ServerFactory serverFactory,WalOpener walOpener){
        this(config,testPortOverride,serverFactory,walOpener,GrpcFollowerClient::new);
    }
    /** Test-only seam: real Netty on port zero with resolved, already-bound peer endpoints. */
    NodeServer(ClusterConfig config,int testPortOverride,
            Function<ClusterConfig,GrpcFollowerClient> outboundClientFactory) {
        this(config,testPortOverride,NodeServer::buildNetty,FileWalStore::open,outboundClientFactory);
    }
    private NodeServer(ClusterConfig config,int testPortOverride,ServerFactory serverFactory,
            WalOpener walOpener,Function<ClusterConfig,GrpcFollowerClient> outboundClientFactory){
        this.outboundClientFactory=Objects.requireNonNull(outboundClientFactory,"outboundClientFactory");
        this.config=Objects.requireNonNull(config,"config");
        this.serverFactory=Objects.requireNonNull(serverFactory,"serverFactory");
        this.walOpener=Objects.requireNonNull(walOpener,"walOpener");
        if(testPortOverride < -1 || testPortOverride > 65535) throw new IllegalArgumentException("invalid test bind port");
        this.port=testPortOverride==-1?config.localBindPort():testPortOverride;
        published=new AtomicReference<>(new NodeStatusSnapshot(config.localNodeId(),config.localRole(),
                NodeLifecycleState.NEW,0,0,0,false));
    }
    public NodeStatusSnapshot statusSnapshot(){return published.get();}
    ReplicationOperationGate operationGateForTest(){return replicationGate;}
    public synchronized int boundPort(){return server!=null && !server.isShutdown()?server.getPort():-1;}

    public synchronized void start() throws IOException {
        if(statusSnapshot().state()!=NodeLifecycleState.NEW)
            throw new IllegalStateException("node may be started only once");
        try {
            transition(NodeLifecycleState.STARTING);
            lease=WalDirectoryLease.acquire(config.walDirectory());
            transition(NodeLifecycleState.RECOVERING);
            WalStore store=walOpener.open(config.walFile());
            log=new WalBackedPersistentLog(store); // one opened WAL; single owner
            WalRecoveryResult recovery=store.recoveryResult();
            published.set(new NodeStatusSnapshot(config.localNodeId(),config.localRole(),
                    NodeLifecycleState.RECOVERING,recovery.entries().size(),recovery.commitIndex(),0,false));
            FollowerReplicationHandler follower=config.localRole()==NodeRole.FOLLOWER
                ? new FollowerReplicationHandler(config,log,store,replicationGate,this::statusSnapshot,
                        this::publishDurableIndex,this::recordFatalStorageFailure) : null;
            if(config.localRole()==NodeRole.LEADER) {
                var outgoing=Objects.requireNonNull(outboundClientFactory.apply(config),"outbound client");
                replicationManager=new ReplicationManager(config,log,outgoing);
                var tracker=new AckTracker(config,log,store);
                var committer=new CommitManager(tracker,store,log,this::publishCommitIndex);
                leaderApplier=new StateMachineApplier(new KvStateMachine());
                leaderCoordinator=new LeaderCoordinator(config,log,store,replicationManager,outgoing,
                        tracker,committer,leaderApplier,this::publishDurableIndex,
                        this::publishAppliedIndex,this::recordFatalStorageFailure);
            }
            var client=new ClientKvGrpcService(config,this::statusSnapshot,leaderCoordinator);
            var peers=new ReplicationGrpcService(config,this::statusSnapshot,follower);
            server=serverFactory.build(config,port,client,peers);
            server.start();
            transition(NodeLifecycleState.CONTROL_ONLY);
            if(follower!=null) replicationGate.open();
            if(leaderCoordinator!=null) leaderCoordinator.openAfterStartup();
        }catch(IOException | RuntimeException failure){
            replicationGate.closeAdmission();
            if(leaderCoordinator!=null)leaderCoordinator.closeAdmission();
            transition(NodeLifecycleState.FAILED);
            try{releaseResourcesAfterTermination();}catch(IOException | RuntimeException cleanup){failure.addSuppressed(cleanup);}
            if(failure instanceof IOException io) throw io;
            throw new IOException("node startup failed",failure);
        }
    }

    private void publishDurableIndex(long lastIndex){
        published.updateAndGet(old->new NodeStatusSnapshot(old.localNodeId(),old.role(),old.state(),
                lastIndex,old.durableCommitIndex(),old.appliedIndex(),false));
    }
    private void publishCommitIndex(long committed) {
        published.updateAndGet(old->new NodeStatusSnapshot(old.localNodeId(),old.role(),old.state(),
                old.lastLogIndex(),committed,old.appliedIndex(),false));
    }
    private void publishAppliedIndex(long applied) {
        published.updateAndGet(old->new NodeStatusSnapshot(old.localNodeId(),old.role(),old.state(),
                old.lastLogIndex(),old.durableCommitIndex(),applied,false));
    }
    private void recordFatalStorageFailure(IOException failure){
        storageFault.compareAndSet(null,failure);
        published.updateAndGet(old->{
            var state=old.state();
            if(state==NodeLifecycleState.STOPPING || state==NodeLifecycleState.STOPPED) return old;
            return old.withState(NodeLifecycleState.FAILED);
        });
    }
    private static Server buildNetty(ClusterConfig config,int port,ClientKvGrpcService client,
            ReplicationGrpcService peers){
        return NettyServerBuilder.forAddress(new InetSocketAddress(config.localBindHost(),port))
                .maxInboundMessageSize(config.maxInboundMessageBytes()).addService(client).addService(peers).build();
    }
    public void awaitTermination() throws InterruptedException {
        Server current;
        synchronized(this){current=server;}
        if(current!=null)current.awaitTermination();
    }
    /** Shutdown must confirm BOTH gRPC termination AND all admitted storage operations exiting. */
    @Override public synchronized void close() throws IOException {
        if(statusSnapshot().state()==NodeLifecycleState.STOPPED)return;
        transition(NodeLifecycleState.STOPPING);
        replicationGate.closeAdmission();
        if(leaderCoordinator!=null)leaderCoordinator.closeAdmission();
        releaseResourcesAfterTermination();
        transition(NodeLifecycleState.STOPPED);
        IOException fault=storageFault.get();
        if(fault!=null) throw new IOException("node closed after fatal replication storage fault",fault);
    }
    private void releaseResourcesAfterTermination() throws IOException {
        if(server!=null){
            server.shutdown();
            boolean terminated;
            try {
                terminated=server.awaitTermination(config.shutdownGraceMs(),TimeUnit.MILLISECONDS);
                if(!terminated){
                    server.shutdownNow();
                    terminated=server.awaitTermination(config.shutdownGraceMs(),TimeUnit.MILLISECONDS);
                }
            }catch(InterruptedException interrupted){
                Thread.currentThread().interrupt();server.shutdownNow();
                throw new IOException("interrupted awaiting gRPC termination; WAL lease retained",interrupted);
            }
            if(!terminated || !server.isTerminated())
                throw new IOException("gRPC termination unconfirmed; WAL lease retained");
            server=null;
        }
        try {
            if(!replicationGate.awaitDrained(config.shutdownGraceMs(),TimeUnit.MILLISECONDS))
                throw new IOException("replication storage operations still active; WAL lease retained");
        }catch(InterruptedException interrupted){
            Thread.currentThread().interrupt();
            throw new IOException("interrupted awaiting WAL operations; WAL lease retained",interrupted);
        }
        // Independent leader mutation permits and outbound worker ownership must finish
        // before the one WAL is closed or the directory lease is released.
        if(leaderCoordinator!=null) {
            try {
                if(!leaderCoordinator.awaitDrained(config.shutdownGraceMs(),TimeUnit.MILLISECONDS))
                    throw new IOException("leader mutation still active; WAL lease retained");
            }catch(InterruptedException interrupted){
                Thread.currentThread().interrupt();
                throw new IOException("interrupted awaiting leader mutation completion; WAL lease retained",interrupted);
            }
        }
        if(replicationManager!=null){
            replicationManager.close(); // bounded wait, preserves channels/WAL lease on failure
            replicationManager=null;
        }
        IOException failure=null;
        if(log!=null){try{log.close();log=null;}catch(IOException e){failure=e;}}
        if(failure==null && lease!=null){try{lease.close();lease=null;}catch(IOException e){failure=e;}}
        if(failure!=null)throw failure;
    }
    private void transition(NodeLifecycleState next){
        published.updateAndGet(old->{
            if(!old.state().canTransitionTo(next))
                throw new IllegalStateException("invalid lifecycle "+old.state()+" -> "+next);
            return old.withState(next);
        });
    }
}
