package io.github.ahmadbitaarr.raftkv.rpc;

import io.grpc.stub.StreamObserver;
import io.github.ahmadbitaarr.raftkv.config.ClusterConfigTest;
import io.github.ahmadbitaarr.raftkv.leader.LeaderReadCoordinator;
import io.github.ahmadbitaarr.raftkv.server.*;
import io.github.ahmadbitaarr.raftkv.statemachine.*;
import io.github.ahmadbitaarr.raftkv.rpc.generated.client.v1.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;

class Phase8GetObserverFailureTest {
    @TempDir Path dir;
    private ClientKvGrpcService ready() {
        var config=ClusterConfigTest.config(dir,"n1");
        var snapshot=new NodeStatusSnapshot("n1",NodeRole.LEADER,NodeLifecycleState.READY,0,0,0,true);
        var read=new LeaderReadCoordinator(new StateMachineApplier(new KvStateMachine()),()->snapshot);
        read.openAfterStartup();
        return new ClientKvGrpcService(config,()->snapshot,null,read);
    }
    @Test void onNextThrowDoesNotCauseSecondTerminalSignal() {
        var service=ready();var next=new AtomicInteger();var completed=new AtomicInteger();var errors=new AtomicInteger();
        var observer=new StreamObserver<GetResponse>(){
            public void onNext(GetResponse v){next.incrementAndGet();throw new IllegalStateException("observer delivery");}
            public void onError(Throwable e){errors.incrementAndGet();}
            public void onCompleted(){completed.incrementAndGet();}
        };
        assertThrows(IllegalStateException.class,()->service.get(GetRequest.newBuilder().setKey("x").build(),observer));
        assertEquals(1,next.get());assertEquals(0,errors.get());assertEquals(0,completed.get());
    }
    @Test void onCompletedThrowDoesNotTriggerOnErrorOrDuplicateCommit() {
        var service=ready();var next=new AtomicInteger();var completed=new AtomicInteger();var errors=new AtomicInteger();
        var observer=new StreamObserver<GetResponse>(){
            public void onNext(GetResponse v){next.incrementAndGet();assertEquals(ResultCode.RESULT_CODE_NOT_FOUND,v.getCode());}
            public void onError(Throwable e){errors.incrementAndGet();}
            public void onCompleted(){completed.incrementAndGet();throw new IllegalStateException("observer terminal");}
        };
        assertThrows(IllegalStateException.class,()->service.get(GetRequest.newBuilder().setKey("x").build(),observer));
        assertEquals(1,next.get());assertEquals(0,errors.get());assertEquals(1,completed.get());
    }
    @Test void invalidGetDoesNotStartReadOrReturnMalformedResponse() {
        var service=ready();var errors=new AtomicInteger();var next=new AtomicInteger();
        var observer=new StreamObserver<GetResponse>(){
            public void onNext(GetResponse r){next.incrementAndGet();}
            public void onCompleted(){}
            public void onError(Throwable t){
                errors.incrementAndGet();
                assertEquals(io.grpc.Status.Code.INVALID_ARGUMENT,io.grpc.Status.fromThrowable(t).getCode());
            }
        };
        service.get(GetRequest.getDefaultInstance(),observer);
        assertEquals(1,errors.get());assertEquals(0,next.get());
    }
}
