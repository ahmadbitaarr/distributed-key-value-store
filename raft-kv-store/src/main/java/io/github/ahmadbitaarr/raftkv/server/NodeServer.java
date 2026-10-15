package io.github.ahmadbitaarr.raftkv.server;

import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.github.ahmadbitaarr.raftkv.config.ClusterConfig;
import io.github.ahmadbitaarr.raftkv.log.WalBackedPersistentLog;
import io.github.ahmadbitaarr.raftkv.rpc.*;
import io.github.ahmadbitaarr.raftkv.wal.*;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/** Single-owner node lifecycle; only CONTROL_ONLY is reachable after a successful Phase 5 start. */
public final class NodeServer implements AutoCloseable {
    @FunctionalInterface
    interface ServerFactory {
        Server build(ClusterConfig config, int port, ClientKvGrpcService client, ReplicationGrpcService replication);
    }
    private final ClusterConfig config;
    private final int port;
    private final ServerFactory serverFactory;
    private volatile NodeStatusSnapshot published;
    private NodeLifecycleState state=NodeLifecycleState.NEW;
    private WalDirectoryLease lease;
    private WalBackedPersistentLog log;
    private Server server;

    public NodeServer(ClusterConfig config){this(config,-1,NodeServer::buildNetty);}
    /** Test-only port 0 override; deployment must use the port validated in ClusterConfig. */
    NodeServer(ClusterConfig config,int testPortOverride){this(config,testPortOverride,NodeServer::buildNetty);}
    NodeServer(ClusterConfig config,int testPortOverride,ServerFactory serverFactory){
        this.config=Objects.requireNonNull(config,"config");
        this.serverFactory=Objects.requireNonNull(serverFactory,"serverFactory");
        if(testPortOverride < -1 || testPortOverride > 65535)throw new IllegalArgumentException("invalid test bind port");
        this.port=testPortOverride==-1?config.localBindPort():testPortOverride;
        published=new NodeStatusSnapshot(config.localNodeId(),config.localRole(),state,0,0,0,false);
    }
    public NodeStatusSnapshot statusSnapshot(){return published;}
    public synchronized int boundPort(){return server!=null && !server.isShutdown()?server.getPort():-1;}
    public synchronized void start() throws IOException {
        if(state!=NodeLifecycleState.NEW)throw new IllegalStateException("node may be started only once");
        try {
            transition(NodeLifecycleState.STARTING);
            lease=WalDirectoryLease.acquire(config.walDirectory());
            transition(NodeLifecycleState.RECOVERING);
            FileWalStore store=FileWalStore.open(config.walFile());
            // Transfer ownership immediately; do not leave two paths closing a single FileChannel.
            log=new WalBackedPersistentLog(store);
            WalRecoveryResult recovery=store.recoveryResult();
            published=new NodeStatusSnapshot(config.localNodeId(),config.localRole(),state,
                    recovery.entries().size(),recovery.commitIndex(),0,false);
            var client=new ClientKvGrpcService(config,this::statusSnapshot);
            var peers=new ReplicationGrpcService(config,this::statusSnapshot);
            server=serverFactory.build(config,port,client,peers);
            server.start();
            transition(NodeLifecycleState.CONTROL_ONLY);
        }catch(IOException | RuntimeException failure){
            transition(NodeLifecycleState.FAILED);
            try{releaseResourcesAfterTermination();}catch(IOException | RuntimeException cleanup){failure.addSuppressed(cleanup);}
            if(failure instanceof IOException io)throw io;
            throw new IOException("node startup failed",failure);
        }
    }
    private static Server buildNetty(ClusterConfig config,int port,ClientKvGrpcService client,ReplicationGrpcService peers){
        return NettyServerBuilder.forAddress(new InetSocketAddress(config.localBindHost(),port))
                .maxInboundMessageSize(config.maxInboundMessageBytes())
                .addService(client).addService(peers).build();
    }
    public void awaitTermination() throws InterruptedException {
        Server current;
        synchronized(this){current=server;}
        if(current!=null)current.awaitTermination();
    }
    /** Publish STOPPING first; never relinquish WAL ownership with active RPC handlers. */
    @Override public synchronized void close() throws IOException {
        if(state==NodeLifecycleState.STOPPED)return;
        transition(NodeLifecycleState.STOPPING);
        releaseResourcesAfterTermination();
        transition(NodeLifecycleState.STOPPED);
    }
    private void releaseResourcesAfterTermination() throws IOException {
        if(server!=null){
            // Close the gRPC admission path before closing storage or releasing the directory lease.
            server.shutdown();
            boolean terminated;
            try{
                terminated=server.awaitTermination(config.shutdownGraceMs(),TimeUnit.MILLISECONDS);
                if(!terminated){
                    server.shutdownNow();
                    terminated=server.awaitTermination(config.shutdownGraceMs(),TimeUnit.MILLISECONDS);
                }
            }catch(InterruptedException e){
                Thread.currentThread().interrupt();
                server.shutdownNow();
                throw new IOException("interrupted while waiting for gRPC termination; WAL ownership retained",e);
            }
            if(!terminated || !server.isTerminated())
                throw new IOException("gRPC server termination not confirmed; WAL ownership retained");
            server=null;
        }
        IOException failure=null;
        if(log!=null){try{log.close();log=null;}catch(IOException e){failure=e;}}
        // A failed WAL close leaves the exclusive lock held; recovery must not race an open handle.
        if(failure==null && lease!=null){try{lease.close();lease=null;}catch(IOException e){failure=e;}}
        if(failure!=null)throw failure;
    }
    private void transition(NodeLifecycleState next){
        if(!state.canTransitionTo(next))throw new IllegalStateException("invalid lifecycle "+state+" -> "+next);
        state=next;
        published=published.withState(next);
    }
}
