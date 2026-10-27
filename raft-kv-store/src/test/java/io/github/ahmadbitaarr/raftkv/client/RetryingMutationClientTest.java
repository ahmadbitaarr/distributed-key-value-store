package io.github.ahmadbitaarr.raftkv.client;

import io.github.ahmadbitaarr.raftkv.config.NodeEndpoint;
import io.github.ahmadbitaarr.raftkv.log.Command;
import org.junit.jupiter.api.Test;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.*;
import static org.junit.jupiter.api.Assertions.*;

class RetryingMutationClientTest {
    private static Map<String,NodeEndpoint> members(){return Map.of("n1",new NodeEndpoint("127.0.0.1",5011),
            "n2",new NodeEndpoint("127.0.0.1",5012));}
    private static RetryingMutationClient.Policy policy(boolean retryNoQuorum){return new RetryingMutationClient.Policy(4,5000,2,20,retryNoQuorum);}
    @Test void generatedRequestIdentityRemainsStableAcrossTransientFailures(){
        var seen=new ArrayList<String>();var values=new ArrayList<byte[]>();var tries=new AtomicInteger();
        var client=new RetryingMutationClient(members(),"n1",(ep,id,cmd,ms)->{
            seen.add(id);values.add(cmd.value().orElseThrow());
            return tries.incrementAndGet()<3?new RetryingMutationClient.Response(RetryingMutationClient.Code.UNAVAILABLE,0,null):
                new RetryingMutationClient.Response(RetryingMutationClient.Code.OK,8,null);
        },policy(false),System::nanoTime,ms->{});
        byte[] input={0,2,4};var result=client.mutate(Command.put("x",input));input[0]=9;
        assertEquals(RetryingMutationClient.Code.OK,result.code());assertEquals(3,result.attempts());
        assertEquals(1,new HashSet<>(seen).size());assertFalse(result.requestId().isBlank());
        for(byte[] bytes:values)assertArrayEquals(new byte[]{0,2,4},bytes);
    }
    @Test void callerSuppliedIdentityIsNotRegenerated(){
        var client=new RetryingMutationClient(members(),"n1",(ep,id,c,ms)->{
            assertEquals("caller-fixed",id);return new RetryingMutationClient.Response(RetryingMutationClient.Code.OK,1,null);
        },policy(false));
        assertEquals("caller-fixed",client.mutate("caller-fixed",Command.delete("x")).requestId());
    }
    @Test void validLeaderHintRoutesOnlyToConfiguredEndpoint(){
        var calls=new ArrayList<NodeEndpoint>();
        var client=new RetryingMutationClient(members(),"n1",(ep,id,cmd,ms)->{
            calls.add(ep);return calls.size()==1
                    ?new RetryingMutationClient.Response(RetryingMutationClient.Code.NOT_LEADER,0,"n1")
                    :new RetryingMutationClient.Response(RetryingMutationClient.Code.OK,4,null);
        },policy(false),System::nanoTime,ms->{});
        assertEquals(RetryingMutationClient.Code.OK,client.mutate("r",Command.delete("x")).code());
        assertEquals(List.of(members().get("n1"),members().get("n1")),calls);
    }
    @Test void followerHintIsRejectedEvenWhenFollowerIsConfiguredMember(){
        var calls=new ArrayList<NodeEndpoint>();
        var client=new RetryingMutationClient(members(),"n1",(ep,id,cmd,ms)->{
            calls.add(ep);
            return new RetryingMutationClient.Response(RetryingMutationClient.Code.NOT_LEADER,0,"n2");
        },policy(false),System::nanoTime,ms->{});
        var result=client.mutate("stable-id",Command.put("x",new byte[]{1}));
        assertEquals(RetryingMutationClient.Code.UNKNOWN_OUTCOME,result.code());
        assertEquals("stable-id",result.requestId());
        assertEquals(1,result.attempts());
        assertEquals(List.of(members().get("n1")),calls);
    }
    @Test void unknownLeaderHintNeverTriggersUnconfiguredSend(){
        var attempts=new AtomicInteger();
        var client=new RetryingMutationClient(members(),"n1",(ep,id,cmd,ms)->{
            attempts.incrementAndGet();return new RetryingMutationClient.Response(RetryingMutationClient.Code.NOT_LEADER,0,"evil");
        },policy(false));
        assertEquals(RetryingMutationClient.Code.UNKNOWN_OUTCOME,client.mutate(Command.delete("x")).code());
        assertEquals(1,attempts.get());
    }
    @Test void deadlineAndNotReadyRetryUseIncreasingBoundedBackoff(){
        var wait=new ArrayList<Long>();var fakeClock=new AtomicLong();
        var client=new RetryingMutationClient(members(),"n1",(ep,id,cmd,ms)->
                new RetryingMutationClient.Response(RetryingMutationClient.Code.NOT_READY,0,null),
                new RetryingMutationClient.Policy(6,100,2,5,false),fakeClock::get,
                ms->{wait.add(ms);fakeClock.addAndGet(TimeUnit.MILLISECONDS.toNanos(ms));});
        var result=client.mutate("retry",Command.delete("x"));
        assertEquals(6,result.attempts());assertEquals(List.of(2L,4L,5L,5L,5L),wait);
        assertEquals(RetryingMutationClient.Code.UNKNOWN_OUTCOME,result.code());
    }
    @Test void conflictAndSuccessTerminateWithoutRetry(){
        for(var code:List.of(RetryingMutationClient.Code.OK,RetryingMutationClient.Code.REQUEST_ID_CONFLICT)){
            var attempts=new AtomicInteger();
            var client=new RetryingMutationClient(members(),"n1",(ep,id,cmd,ms)->{
                attempts.incrementAndGet();return new RetryingMutationClient.Response(code,code==RetryingMutationClient.Code.OK?5:0,null);
            },policy(true));
            assertEquals(code,client.mutate(Command.delete("x")).code());assertEquals(1,attempts.get());
        }
    }
    @Test void noQuorumNeverRetriesWithoutExplicitPolicy(){
        var tries=new AtomicInteger();
        var client=new RetryingMutationClient(members(),"n1",(ep,id,cmd,ms)->{
            tries.incrementAndGet();return new RetryingMutationClient.Response(RetryingMutationClient.Code.NO_QUORUM,0,null);
        },policy(false));
        assertEquals(RetryingMutationClient.Code.NO_QUORUM,client.mutate(Command.delete("x")).code());
        assertEquals(1,tries.get());
    }
    @Test void noQuorumRetryIsBoundedAndKeepsOriginalId(){
        var ids=new ArrayList<String>();
        var client=new RetryingMutationClient(members(),"n1",(ep,id,cmd,ms)->{
            ids.add(id);return new RetryingMutationClient.Response(RetryingMutationClient.Code.NO_QUORUM,0,null);
        },policy(true),System::nanoTime,ms->{});
        var result=client.mutate("same",Command.delete("x"));
        assertEquals(4,result.attempts());assertEquals(RetryingMutationClient.Code.UNKNOWN_OUTCOME,result.code());
        assertEquals(List.of("same","same","same","same"),ids);
    }
    @Test void exhaustedDeadlineReportsUnknownWhilePreservingId(){
        var fakeClock=new AtomicLong();
        var client=new RetryingMutationClient(members(),"n1",(ep,id,cmd,ms)->{
            fakeClock.addAndGet(TimeUnit.SECONDS.toNanos(3));
            return new RetryingMutationClient.Response(RetryingMutationClient.Code.DEADLINE_EXCEEDED,0,null);
        },new RetryingMutationClient.Policy(8,2000,1,4,true),fakeClock::get,ms->{});
        var result=client.mutate("same",Command.delete("x"));
        assertEquals("same",result.requestId());assertEquals(1,result.attempts());
        assertEquals(RetryingMutationClient.Code.UNKNOWN_OUTCOME,result.code());
    }
}
