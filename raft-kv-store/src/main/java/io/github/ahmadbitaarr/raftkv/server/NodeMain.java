package io.github.ahmadbitaarr.raftkv.server;

import io.github.ahmadbitaarr.raftkv.config.ConfigLoader;
/** Minimal foreground process entrypoint; no retries, discovery, elections, or background work. */
public final class NodeMain {
    private NodeMain(){}
    public static void main(String[] args){
        NodeServer node=null;
        try{
            var path=ConfigLoader.resolvePath(args,System.getenv());
            node=new NodeServer(ConfigLoader.load(path));
            NodeServer owner=node;
            Runtime.getRuntime().addShutdownHook(new Thread(()->{
                try{owner.close();}catch(Exception e){System.err.println("Node shutdown failed: "+e);}
            },"raftkv-node-shutdown"));
            node.start();
            System.out.println("Node listener bound: "+node.boundPort()+" state="+node.statusSnapshot().state()+" dataReady="+node.statusSnapshot().ready());
            node.awaitTermination();
            node.close();
        }catch(Exception failure){
            System.err.println("Node failed: "+failure);
            if(node!=null){try{node.close();}catch(Exception cleanup){failure.addSuppressed(cleanup);}}
            System.exit(1);
        }
    }
}
