package io.github.ahmadbitaarr.raftkv.client;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.github.ahmadbitaarr.raftkv.config.NodeEndpoint;
import io.github.ahmadbitaarr.raftkv.log.Command;
import io.github.ahmadbitaarr.raftkv.rpc.ClientContractValidator;
import io.github.ahmadbitaarr.raftkv.rpc.ReplicationContractValidator;
import io.github.ahmadbitaarr.raftkv.rpc.generated.client.v1.*;
import io.github.ahmadbitaarr.raftkv.rpc.generated.replication.v1.*;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/** Small, deterministic, machine-readable gRPC smoke command (no HTTP endpoint or WAL access). */
public final class DeploymentSmokeClient {
    private DeploymentSmokeClient() { }
    public static final String USAGE="operation: put|delete|get|status|retry-put|retry-delete; --host --port --key --request-id --value-base64 --leader --expect --expect-index --expect-value-base64";
    public record Options(String operation, String host, int port, String key, String requestId,
            byte[] value, String leaderId, String expectCode, Long expectIndex, byte[] expectValue) {
        public Options {value=value==null?null:value.clone();expectValue=expectValue==null?null:expectValue.clone();}
        @Override public byte[] value(){return value==null?null:value.clone();}
        @Override public byte[] expectValue(){return expectValue==null?null:expectValue.clone();}
    }
    public static Options parse(String[] args) {
        if (args.length<1 || !Set.of("put","delete","get","status","retry-put","retry-delete").contains(args[0]))
            throw new IllegalArgumentException(USAGE);
        Map<String,String> values=new HashMap<>();
        Set<String> supported=Set.of("host","port","key","request-id","value-base64","leader","expect","expect-index","expect-value-base64");
        for(int i=1;i<args.length;i++) {
            String arg=args[i];
            if(!arg.startsWith("--") || !arg.contains("="))throw new IllegalArgumentException("expected --name=value");
            String[] parts=arg.substring(2).split("=",2);
            if(!supported.contains(parts[0]) || values.putIfAbsent(parts[0],parts[1])!=null)
                throw new IllegalArgumentException("unknown/duplicate option: "+parts[0]);
        }
        String op=args[0];
        String host=values.getOrDefault("host","node-1");
        if(host.isBlank() || host.contains("/"))throw new IllegalArgumentException("invalid host");
        int port=Integer.parseInt(values.getOrDefault("port","50051"));
        if(port<1 || port>65535)throw new IllegalArgumentException("invalid port");
        String key=values.get("key");
        if(!op.equals("status") && (key==null || key.isBlank()))throw new IllegalArgumentException("--key required");
        String id=values.getOrDefault("request-id",UUID.randomUUID().toString());
        if(!op.equals("get") && !op.equals("status") && id.isBlank())throw new IllegalArgumentException("invalid request ID");
        if((op.equals("put") || op.equals("retry-put")) && !values.containsKey("value-base64"))
            throw new IllegalArgumentException("PUT needs explicit --value-base64= (empty allowed)");
        byte[] value=values.containsKey("value-base64")?Base64.getDecoder().decode(values.get("value-base64")):null;
        byte[] expected=values.containsKey("expect-value-base64")?Base64.getDecoder().decode(values.get("expect-value-base64")):null;
        Long index=values.containsKey("expect-index")?Long.parseLong(values.get("expect-index")):null;
        return new Options(op,host,port,key,id,value,values.getOrDefault("leader","n1"),values.get("expect"),index,expected);
    }
    // JSON output has bounded schema and escaped user strings; scripts parse it, not human-readable CLI decorations.
    static String quote(String input) {
        StringBuilder result=new StringBuilder("\"");
        for(int i=0;i<input.length();i++) {
            char c=input.charAt(i);
            switch(c) {
                case '"' -> result.append("\\\"");
                case '\\' -> result.append("\\\\");
                case '\n' -> result.append("\\n");
                case '\r' -> result.append("\\r");
                case '\t' -> result.append("\\t");
                default -> {if(c<32)result.append(String.format("\\u%04x",(int)c));else result.append(c);}
            }
        }
        return result.append('"').toString();
    }
    public static boolean matches(String actualCode,Long actualIndex,byte[] actualValue,Options opts) {
        if(opts.expectCode()!=null && !opts.expectCode().replaceFirst("^RESULT_CODE_", "").equals(actualCode))return false;
        if(opts.expectIndex()!=null && !opts.expectIndex().equals(actualIndex))return false;
        return opts.expectValue()==null || java.util.Arrays.equals(opts.expectValue(),actualValue);
    }
    public static int execute(String[] args,PrintStream out,PrintStream err) {
        try {return run(parse(args),out,err);}
        catch(Exception failed) {err.println("smoke operation failed: "+failed.getMessage());return 2;}
    }
    private static int run(Options o,PrintStream out,PrintStream err) throws Exception {
        ManagedChannel channel=ManagedChannelBuilder.forAddress(o.host(),o.port()).usePlaintext().build();
        try {
            String code;
            Long index=null;
            byte[] value=null;
            boolean hasValue=false;
            String requestId=o.requestId();
            long last=0,commit=0,applied=0;
            String node="", role="";
            if(o.operation().startsWith("retry-")) {
                Command command=o.operation().equals("retry-put")?Command.put(o.key(),o.value()):Command.delete(o.key());
                try (GrpcMutationTransport transport=new GrpcMutationTransport()) {
                    // Leader is immutable; the supplied host is the expected configured-leader endpoint.
                    var retry=new RetryingMutationClient(Map.of(o.leaderId(),new NodeEndpoint(o.host(),o.port())),o.leaderId(),
                            transport,new RetryingMutationClient.Policy(4,10000,50,500,false));
                    var result=retry.mutate(requestId,command);
                    code=result.code().name();
                    if(result.code()==RetryingMutationClient.Code.OK)index=result.commitIndex();
                }
            } else if(o.operation().equals("status")) {
                var request=ReplicationStatusRequest.newBuilder().setLeaderId(o.leaderId()).build();
                var response=ReplicationServiceGrpc.newBlockingStub(channel).withDeadlineAfter(4,TimeUnit.SECONDS)
                        .getReplicationStatus(request);
                ReplicationContractValidator.validate(response);
                code="OK";node=response.getNodeId();role=response.getRole().name();
                last=response.getLastLogIndex();commit=response.getCommitIndex();applied=response.getAppliedIndex();
            } else if(o.operation().equals("get")) {
                var request=GetRequest.newBuilder().setKey(o.key()).build();
                var response=KeyValueServiceGrpc.newBlockingStub(channel).withDeadlineAfter(5,TimeUnit.SECONDS).get(request);
                ClientContractValidator.validate(response,request);
                code=response.getCode().name();
                if(response.hasCommitIndex())index=response.getCommitIndex();
                hasValue=response.hasValue();
                if(hasValue)value=response.getValue().toByteArray();
            } else {
                var stub=KeyValueServiceGrpc.newBlockingStub(channel).withDeadlineAfter(7,TimeUnit.SECONDS);
                MutationResponse response;
                if(o.operation().equals("put")) {
                    var request=PutRequest.newBuilder().setRequestId(requestId).setKey(o.key())
                            .setValue(ByteString.copyFrom(o.value())).build();
                    ClientContractValidator.validate(request);
                    response=stub.put(request);
                    ClientContractValidator.validate(response,request);
                } else {
                    var request=DeleteRequest.newBuilder().setRequestId(requestId).setKey(o.key()).build();
                    ClientContractValidator.validate(request);
                    response=stub.delete(request);
                    ClientContractValidator.validate(response,request);
                }
                code=response.getCode().name();
                if(response.hasCommitIndex())index=response.getCommitIndex();
            }
            String normalized=code.replaceFirst("^RESULT_CODE_","");
            out.println("{\"code\":"+quote(normalized)+",\"request_id\":"+quote(requestId)
                    +",\"commit_index\":"+(index==null?"null":index)+",\"has_value\":"+hasValue
                    +",\"value_base64\":"+(value==null?"null":quote(Base64.getEncoder().encodeToString(value)))
                    +",\"node_id\":"+quote(node)+",\"role\":"+quote(role)
                    +",\"last_log_index\":"+last+",\"durable_commit_index\":"+commit+",\"applied_index\":"+applied+"}");
            if(!matches(normalized,index,value,o)){
                err.println("smoke assertion failed: expected="+o.expectCode()+" index="+o.expectIndex());
                return 1;
            }
            return 0;
        } finally {
            channel.shutdownNow();
            try {channel.awaitTermination(2,TimeUnit.SECONDS);}catch(InterruptedException interrupted){Thread.currentThread().interrupt();}
        }
    }
    public static void main(String[] args) {int exit=execute(args,System.out,System.err);if(exit!=0)System.exit(exit);}
}
