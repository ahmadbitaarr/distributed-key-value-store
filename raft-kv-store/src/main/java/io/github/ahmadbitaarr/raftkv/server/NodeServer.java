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
import io.github.ahmadbitaarr.raftkv.replication.FollowerCatchUpCoordinator;
import io.github.ahmadbitaarr.raftkv.recovery.RecoveryManager;
import io.github.ahmadbitaarr.raftkv.commit.AckTracker;
import io.github.ahmadbitaarr.raftkv.commit.CommitManager;
import io.github.ahmadbitaarr.raftkv.leader.LeaderCoordinator;
import io.github.ahmadbitaarr.raftkv.leader.LeaderReadCoordinator;
import io.github.ahmadbitaarr.raftkv.statemachine.KvStateMachine;
import io.github.ahmadbitaarr.raftkv.statemachine.StateMachineApplier;
import io.github.ahmadbitaarr.raftkv.rpc.*;
import io.github.ahmadbitaarr.raftkv.wal.*;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/** Single WAL owner: leader READY, follower CONTROL_ONLY with durable catch-up and bounded cleanup. */
public final class NodeServer implements AutoCloseable {
    @FunctionalInterface interface ServerFactory {
        Server build(ClusterConfig config,int port,ClientKvGrpcService client,ReplicationGrpcService replication);
    }
    @FunctionalInterface interface WalOpener { WalStore open(java.nio.file.Path path) throws IOException; }
    /** Test-only barrier at the bound-listener / leader-admission boundary. */
    @FunctionalInterface interface StartupBoundary { void run(int boundPort,Consumer<IOException> storageFailure) throws IOException; }

    private final ClusterConfig config;
    private final int port;
    private final ServerFactory serverFactory;
    private final WalOpener walOpener;
    private final Function<ClusterConfig,? extends io.github.ahmadbitaarr.raftkv.replication.FollowerMaintenanceClient> outboundClientFactory;
    private final StartupBoundary startupBoundary;
    private final ReplicationOperationGate replicationGate=new ReplicationOperationGate();
    private final AtomicReference<NodeStatusSnapshot> published;
    private final AtomicReference<IOException> storageFault=new AtomicReference<>();
    private WalDirectoryLease lease;
    private WalBackedPersistentLog log;
    private Server server;
    private ReplicationManager replicationManager;
    private LeaderCoordinator leaderCoordinator;
    private StateMachineApplier leaderApplier;
    private StateMachineApplier followerApplier;
    private FollowerCatchUpCoordinator leaderCatchUp;
    private LeaderReadCoordinator leaderReads;
    private RecoveryManager recoveryManager;
    /** Independent of the synchronized NodeServer lifetime monitor: worker callbacks never need it. */
    private final Object recoveryActivationLock=new Object();
    private boolean recoveryActivationClosed;

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
            Function<ClusterConfig,? extends io.github.ahmadbitaarr.raftkv.replication.FollowerMaintenanceClient> outboundClientFactory) {
        this(config,testPortOverride,NodeServer::buildNetty,FileWalStore::open,outboundClientFactory);
    }
    NodeServer(ClusterConfig config,int testPortOverride,
            Function<ClusterConfig,? extends io.github.ahmadbitaarr.raftkv.replication.FollowerMaintenanceClient> outboundClientFactory,StartupBoundary startupBoundary) {
        this(config,testPortOverride,NodeServer::buildNetty,FileWalStore::open,outboundClientFactory,startupBoundary);
    }
    /** Package-local test seam: real Netty with an injected WAL force boundary and real peer channels. */
    NodeServer(ClusterConfig config,int testPortOverride,WalOpener walOpener,
            Function<ClusterConfig,? extends io.github.ahmadbitaarr.raftkv.replication.FollowerMaintenanceClient> outboundClientFactory){
        this(config,testPortOverride,NodeServer::buildNetty,walOpener,outboundClientFactory);
    }
    private NodeServer(ClusterConfig config,int testPortOverride,ServerFactory serverFactory,
            WalOpener walOpener,Function<ClusterConfig,? extends io.github.ahmadbitaarr.raftkv.replication.FollowerMaintenanceClient> outboundClientFactory){
        this(config,testPortOverride,serverFactory,walOpener,outboundClientFactory,(ignoredPort,ignoredFailure) -> {});
    }
    private NodeServer(ClusterConfig config,int testPortOverride,ServerFactory serverFactory,
            WalOpener walOpener,Function<ClusterConfig,? extends io.github.ahmadbitaarr.raftkv.replication.FollowerMaintenanceClient> outboundClientFactory,
            StartupBoundary startupBoundary){
        this.outboundClientFactory=Objects.requireNonNull(outboundClientFactory,"outboundClientFactory");
        this.startupBoundary=Objects.requireNonNull(startupBoundary,"startupBoundary");
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
    LeaderReadCoordinator readCoordinatorForTest(){return leaderReads;}
    LeaderCoordinator leaderCoordinatorForTest(){return leaderCoordinator;}
    StateMachineApplier readApplierForTest(){return leaderApplier;}
    StateMachineApplier followerApplierForTest(){return followerApplier;}
    RecoveryManager recoveryManagerForTest(){return recoveryManager;}
    public synchronized int boundPort(){return server!=null && !server.isShutdown()?server.getPort():-1;}

    public synchronized void start() throws IOException {
        if(statusSnapshot().state()!=NodeLifecycleState.NEW)
            throw new IllegalStateException("node may be started only once");
        try {
            transition(NodeLifecycleState.STARTING);
            lease=WalDirectoryLease.acquire(config.walDirectory());
            transition(NodeLifecycleState.RECOVERING);
            // A preexisting zero-length WAL can be the durable result of truncating an
            // unresolved leader tail. Only an ABSENT WAL proves a fresh initialization.
            // Unknown filesystem state also fails closed (Files.notExists returns false).
            final boolean walAbsentBeforeOpen=java.nio.file.Files.notExists(
                    config.walFile(),java.nio.file.LinkOption.NOFOLLOW_LINKS);
            WalStore store=walOpener.open(config.walFile());
            log=new WalBackedPersistentLog(store); // one opened WAL; single owner
            WalRecoveryResult recovery=store.recoveryResult();
            // Capture READY eligibility from recovered WAL BEFORE opening mutation admission.
            final boolean freshLeaderAtRecovery=config.localRole()==NodeRole.LEADER
                    && walAbsentBeforeOpen && recovery.entries().isEmpty() && recovery.commitIndex()==0;
            published.set(new NodeStatusSnapshot(config.localNodeId(),config.localRole(),
                    NodeLifecycleState.RECOVERING,recovery.entries().size(),recovery.commitIndex(),0,false));
            // Phase 9 narrowly replays follower-only committed history before any RPC admission.
            FollowerReplicationHandler follower=null;
            if(config.localRole()==NodeRole.FOLLOWER) {
                followerApplier=new StateMachineApplier(new KvStateMachine());
                try {
                    long restored=followerApplier.applyThrough(recovery.commitIndex(),recovery.commitIndex(),log);
                    publishAppliedIndex(restored);
                }catch(RuntimeException | Error invalidReplay) {
                    throw new IOException("follower committed WAL replay failed; admission remains closed",invalidReplay);
                }
                follower=new FollowerReplicationHandler(config,log,store,replicationGate,this::statusSnapshot,
                        this::publishDurableIndex,followerApplier,this::publishCommitIndex,
                        this::publishAppliedIndex,this::recordFatalStorageFailure);
            }
            if(config.localRole()==NodeRole.LEADER) {
                var outgoing=Objects.requireNonNull(outboundClientFactory.apply(config),"outbound client");
                replicationManager=new ReplicationManager(config,log,outgoing);
                var tracker=new AckTracker(config,log,store);
                var committer=new CommitManager(tracker,store,log,this::publishCommitIndex);
                leaderApplier=new StateMachineApplier(new KvStateMachine());
                if(!freshLeaderAtRecovery) {
                    // Canonicalize the one owned leader WAL before any listener or data gate opens.
                    // Both metadata and replay are published as one immutable not-ready snapshot.
                    var local=RecoveryManager.recoverLocal(log,store,leaderApplier);
                    published.set(new NodeStatusSnapshot(config.localNodeId(),config.localRole(),
                            NodeLifecycleState.RECOVERING,local.committedIndex(),
                            local.committedIndex(),local.appliedIndex(),false));
                }
                if(leaderApplier.appliedIndex()!=store.durableCommitIndex())
                    throw new IOException("leader replay/application progress mismatch");
                leaderReads=new LeaderReadCoordinator(leaderApplier,this::readSnapshotForRead);
                leaderCoordinator=new LeaderCoordinator(config,log,store,replicationManager,outgoing,
                        tracker,committer,leaderApplier,this::publishDurableIndex,
                        this::publishAppliedIndex,this::recordFatalStorageFailure);
                leaderCatchUp=new FollowerCatchUpCoordinator(config,log,this::statusSnapshot,replicationManager,
                        outgoing,tracker,leaderCoordinator::hasUnresolvedSuffix,
                        fault -> {if(!freshLeaderAtRecovery)recordFatalStorageFailure(fault);});
                leaderCoordinator.setCommitWakeup(leaderCatchUp::wake);
                if(!freshLeaderAtRecovery) {
                    recoveryManager=new RecoveryManager(config,log,store,leaderApplier,
                            replicationManager,outgoing,tracker,this::activateRecoveredLeader,
                            this::recordFatalStorageFailure);
                }
            }
            var client=new ClientKvGrpcService(config,this::statusSnapshot,leaderCoordinator,leaderReads);
            var peers=new ReplicationGrpcService(config,this::statusSnapshot,follower);
            server=serverFactory.build(config,port,client,peers);
            server.start();
            transition(NodeLifecycleState.CONTROL_ONLY);
            if(follower!=null) replicationGate.open();
            if(leaderCoordinator!=null) {
                // The listener is live, but no leader mutation can append while READY is published.
                startupBoundary.run(server.getPort(),this::recordFatalStorageFailure);
                requireHealthyLeaderStartup();
                if(freshLeaderAtRecovery) {
                    leaderReads.openAfterStartup();
                    requireHealthyLeaderStartup();
                    transition(NodeLifecycleState.READY);
                    requireHealthyLeaderStartup();
                    // Existing fresh-leader rule: open writer LAST, after READY publication.
                    leaderCoordinator.openAfterStartup();
                    requireHealthyLeaderStartup();
                    if(leaderCatchUp!=null)leaderCatchUp.start();
                } else {
                    // Only control RPCs are available. One independently verified follower
                    // will authorize readiness asynchronously; no indefinite startup wait.
                    recoveryManager.start();
                }
            }
        }catch(IOException | RuntimeException failure){
            synchronized(recoveryActivationLock) {
                recoveryActivationClosed=true;
                if(recoveryManager!=null)recoveryManager.closeAdmission();
            }
            replicationGate.closeAdmission();
            if(leaderReads!=null)leaderReads.closeAdmission();
            if(leaderCoordinator!=null)leaderCoordinator.closeAdmission();
            if(statusSnapshot().state()!=NodeLifecycleState.FAILED)transition(NodeLifecycleState.FAILED);
            try{releaseResourcesAfterTermination();}catch(IOException | RuntimeException cleanup){failure.addSuppressed(cleanup);}
            if(failure instanceof IOException io) throw io;
            throw new IOException("node startup failed",failure);
        }
    }

    /** Recovery completion cannot race STOPPING; never acquires the NodeServer monitor. */
    private void activateRecoveredLeader(RecoveryManager.Certificate certificate) {
        synchronized(recoveryActivationLock) {
            if(recoveryActivationClosed || storageFault.get()!=null || recoveryManager==null
                    || !recoveryManager.accepts(certificate)
                    || published.get().state()!=NodeLifecycleState.CONTROL_ONLY)
                return;
            try {
                requireHealthyLeaderStartup();
                leaderReads.openAfterStartup();
                requireHealthyLeaderStartup();
                if(recoveryActivationClosed || published.get().state()!=NodeLifecycleState.CONTROL_ONLY)
                    return;
                transition(NodeLifecycleState.READY);
                requireHealthyLeaderStartup();
                // A recovered-leader writer requires the issued, owner-bound certificate.
                leaderCoordinator.openAfterVerifiedRecovery(certificate);
                requireHealthyLeaderStartup();
                leaderCatchUp.start();
            } catch(IOException | RuntimeException failedActivation) {
                IOException error=failedActivation instanceof IOException io ? io
                        : new IOException("recovered leader activation failed",failedActivation);
                recordFatalStorageFailure(error);
            }
        }
    }

    private void requireHealthyLeaderStartup() throws IOException {
        IOException failure=storageFault.get();
        if(failure==null && leaderCoordinator!=null)failure=leaderCoordinator.storageFailure();
        if(failure!=null)throw new IOException("fatal storage failure during leader startup",failure);
    }

    /** A latched storage failure is never obscured by a racing READY publication. */
    private NodeStatusSnapshot readSnapshotForRead(){
        var current=published.get();
        if(current.state()==NodeLifecycleState.READY
                && (storageFault.get()!=null || (leaderCoordinator!=null
                        && leaderCoordinator.storageFailure()!=null)))
            return current.withState(NodeLifecycleState.FAILED);
        return current;
    }

    private void publishDurableIndex(long lastIndex){
        published.updateAndGet(old->new NodeStatusSnapshot(old.localNodeId(),old.role(),old.state(),
                lastIndex,old.durableCommitIndex(),old.appliedIndex(),old.ready()));
    }
    private void publishCommitIndex(long committed) {
        published.updateAndGet(old->new NodeStatusSnapshot(old.localNodeId(),old.role(),old.state(),
                old.lastLogIndex(),committed,old.appliedIndex(),old.ready()));
    }
    private void publishAppliedIndex(long applied) {
        published.updateAndGet(old->new NodeStatusSnapshot(old.localNodeId(),old.role(),old.state(),
                old.lastLogIndex(),old.durableCommitIndex(),applied,old.ready()));
    }
    private void recordFatalStorageFailure(IOException failure){
        storageFault.compareAndSet(null,failure);
        if(leaderReads!=null)leaderReads.closeAdmission();
        if(recoveryManager!=null)recoveryManager.closeAdmission();
        if(leaderCoordinator!=null)leaderCoordinator.failExternally(failure);
        if(leaderCatchUp!=null)leaderCatchUp.closeAdmission();
        replicationGate.closeAdmission();
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
        synchronized(recoveryActivationLock) {
            recoveryActivationClosed=true;
            transition(NodeLifecycleState.STOPPING);
            if(recoveryManager!=null)recoveryManager.closeAdmission();
            if(leaderReads!=null)leaderReads.closeAdmission();
        }
        if(leaderCatchUp!=null)leaderCatchUp.closeAdmission();
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
        // An accepted GET retains its permit until state access actually completes.
        if(leaderReads!=null) {
            try {
                if(!leaderReads.awaitDrained(config.shutdownGraceMs(),TimeUnit.MILLISECONDS))
                    throw new IOException("leader read still active; WAL lease retained");
            } catch(InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted awaiting leader read drainage; WAL lease retained",interrupted);
            }
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
        if(recoveryManager!=null) {
            recoveryManager.close(); // bounded and confirmed before any outbound channel or WAL close
            recoveryManager=null;
        }
        if(leaderCatchUp!=null) {
            leaderCatchUp.close(); // actual catch-up workers must exit before channels/WAL ownership ends
            leaderCatchUp=null;
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
