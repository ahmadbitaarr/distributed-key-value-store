package io.github.ahmadbitaarr.raftkv.config;

import io.github.ahmadbitaarr.raftkv.rpc.RpcLimits;
import java.io.*;
import java.nio.file.*;
import java.util.*;

/** Strict properties loader; no mutable per-field environment leadership overrides. */
public final class ConfigLoader {
    private ConfigLoader(){}
    public static Path resolvePath(String[] args, Map<String,String> env){
        Objects.requireNonNull(args,"args");Objects.requireNonNull(env,"env");
        String selected=null;
        for(String arg:args){
            if(!arg.startsWith("--config=") || selected!=null)throw new IllegalArgumentException("usage: --config=PATH");
            selected=arg.substring("--config=".length());
        }
        if(selected==null) selected=env.get("RAFTKV_CONFIG");
        if(selected==null || selected.isBlank())throw new IllegalArgumentException("--config=PATH or RAFTKV_CONFIG required");
        return Path.of(selected).toAbsolutePath().normalize();
    }
    public static ClusterConfig load(Path path) throws IOException {
        Objects.requireNonNull(path,"configuration path");
        Properties props=new Properties(){
            @Override public synchronized Object put(Object key,Object value){
                if(containsKey(key))throw new IllegalArgumentException("duplicate property: "+key);
                return super.put(key,value);
            }
        };
        try(Reader reader=Files.newBufferedReader(path,java.nio.charset.StandardCharsets.UTF_8)){props.load(reader);}
        Set<String> required=new HashSet<>(Set.of("cluster.id","cluster.leader.id","local.node.id","server.bind.host","server.bind.port","storage.wal.directory","rpc.client.deadline.ms","rpc.replication.deadline.ms","rpc.status.deadline.ms","server.shutdown.grace.ms","rpc.max.inbound.bytes"));
        Map<String,NodeEndpoint> members=new LinkedHashMap<>();
        for(String key:props.stringPropertyNames()){
            if(key.startsWith("cluster.member.")){
                String id=key.substring("cluster.member.".length());
                if(id.isBlank())throw new IllegalArgumentException("missing member ID");
                String address=props.getProperty(key);
                int colon=address.lastIndexOf(':');
                if(colon<=0 || colon==address.length()-1 || address.indexOf(':')!=colon)
                    throw new IllegalArgumentException("member endpoint must be host:port: "+id);
                members.put(id,new NodeEndpoint(address.substring(0,colon),integer(address.substring(colon+1),key)));
            } else if(!required.remove(key)) throw new IllegalArgumentException("unknown property: "+key);
        }
        if(!required.isEmpty())throw new IllegalArgumentException("missing properties: "+required);
        String rawDirectory = props.getProperty("storage.wal.directory");
        if (rawDirectory.isBlank()) throw new IllegalArgumentException("WAL directory must not be blank");
        Path directory = Path.of(rawDirectory).toAbsolutePath().normalize();

        Path candidate = directory.getRoot();

        for (Path component : directory) {
            candidate = candidate.resolve(component);

            try {
                var attributes = Files.readAttributes(
                        candidate,
                        java.nio.file.attribute.BasicFileAttributes.class
                );

                if (!attributes.isDirectory()) {
                    throw new IllegalArgumentException(
                            "WAL path component is not a directory: " + candidate
                    );
                }

            } catch (NoSuchFileException e) {
                // This component does not exist yet.
                // Descendants can be created during startup.
                break;

            } catch (IOException e) {
                throw new IllegalArgumentException(
                        "Cannot inspect WAL path component: " + candidate, e
                );
            }
        }

        if (Files.exists(directory.resolve("raftkv.wal"))
                && !Files.isRegularFile(directory.resolve("raftkv.wal"))) {
            throw new IllegalArgumentException("WAL file path is not a file");
        }

        return new ClusterConfig(props.getProperty("cluster.id"),props.getProperty("local.node.id"),
            props.getProperty("cluster.leader.id"),members,props.getProperty("server.bind.host"),
            integer(props.getProperty("server.bind.port"),"server.bind.port"),directory,
            integer(props.getProperty("rpc.client.deadline.ms"),"client deadline"),
            integer(props.getProperty("rpc.replication.deadline.ms"),"replication deadline"),
            integer(props.getProperty("rpc.status.deadline.ms"),"status deadline"),
            integer(props.getProperty("server.shutdown.grace.ms"),"shutdown grace"),
            integer(props.getProperty("rpc.max.inbound.bytes"),"max inbound"));
    }
    private static int integer(String input,String name){try{return Integer.parseInt(input);}catch(NumberFormatException e){throw new IllegalArgumentException("invalid integer for "+name,e);}}
}
