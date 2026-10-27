package io.github.ahmadbitaarr.raftkv.leader;

import io.github.ahmadbitaarr.raftkv.log.Command;
import io.github.ahmadbitaarr.raftkv.log.LogEntry;
import io.github.ahmadbitaarr.raftkv.log.OperationType;
import io.github.ahmadbitaarr.raftkv.rpc.RpcLimits;
import io.github.ahmadbitaarr.raftkv.log.WalBackedPersistentLog;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Process-local single-owner table plus a committed index reconstructed only from the durable WAL. */
public final class RequestDeduplicator {
    public enum Kind { NEW, JOIN, COMMITTED_DUPLICATE, CONFLICT, UNRESOLVED, CLOSED }
    public enum CompletionCode { OK, NOT_READY, NO_QUORUM }
    public record Completion(CompletionCode code, long commitIndex) {
        public Completion {
            Objects.requireNonNull(code);
            if((code==CompletionCode.OK)!=(commitIndex>0))
                throw new IllegalArgumentException("invalid request completion");
        }
    }
    private record Identity(Command command, byte[] fingerprint) {
        private Identity {
            Objects.requireNonNull(command);
            fingerprint=Arrays.copyOf(Objects.requireNonNull(fingerprint),fingerprint.length);
        }
        boolean matches(Command other, byte[] hash) {
            return command.equals(other) && Arrays.equals(fingerprint,hash);
        }
    }
    private record Committed(Identity identity,long index) { }
    private static final class Active {
        final Identity identity;
        final CompletableFuture<Completion> completion=new CompletableFuture<>();
        Active(Identity identity){this.identity=identity;}
    }
    public final class Owner {
        private final String id;
        private final Active active;
        private Owner(String id,Active active){this.id=id;this.active=active;}
    }
    public record Decision(Kind kind,long commitIndex, CompletableFuture<Completion> completion, Owner owner) {
        public Decision { Objects.requireNonNull(kind); }
    }
    private final Map<String,Committed> committed=new HashMap<>();
    private final Map<String,Active> active=new HashMap<>();
    private final Map<String,Identity> unresolved=new HashMap<>();
    private boolean closed;

    /** Recovery validation is fail-closed: never guess which of two committed IDs should win. */
    public synchronized void reconstruct(WalBackedPersistentLog log,long committedIndex) throws IOException {
        Objects.requireNonNull(log);
        if(closed || !active.isEmpty() || !committed.isEmpty() || !unresolved.isEmpty())
            throw new IllegalStateException("reconstruction requires a fresh deduplicator");
        if(committedIndex<0 || log.lastLogIndex()<committedIndex)
            throw new IOException("invalid committed history boundary");
        Map<String,Committed> restored=new HashMap<>();
        for(long index=1;index<=committedIndex;index++) {
            LogEntry entry=log.get(index).orElseThrow(()->new IOException("missing committed entry"));
            if(entry.index()!=index)throw new IOException("invalid committed entry index");
            Command command=entry.command();
            byte[] canonical;
            try {
                RpcLimits.validText(entry.requestId(),RpcLimits.MAX_REQUEST_ID_BYTES,"request_id",true);
                RpcLimits.validText(command.key(),RpcLimits.MAX_KEY_BYTES,"key",false);
                if(command.type()==OperationType.PUT) {
                    byte[] value=command.value().orElseThrow();
                    if(value.length>RpcLimits.MAX_VALUE_BYTES)throw new IllegalArgumentException("oversized PUT value");
                }else if(command.type()!=OperationType.DELETE || command.value().isPresent()) {
                    throw new IllegalArgumentException("invalid committed operation");
                }
                canonical=CanonicalCommandFingerprint.of(command);
            }catch(RuntimeException error){throw new IOException("invalid committed command or identity at "+index,error);}
            if(!Arrays.equals(canonical,entry.commandFingerprint()))
                throw new IOException("committed request fingerprint mismatch at "+index);
            if(restored.putIfAbsent(entry.requestId(),new Committed(new Identity(command,canonical),index))!=null)
                throw new IOException("duplicate committed request ID in WAL at "+index);
        }
        committed.putAll(restored);
    }
    /** One monitor atomically arbitrates new ownership, joins, conflicts and completed commits. */
    public synchronized Decision classify(String id,Command command,byte[] fingerprint) {
        if(id==null || id.isBlank())throw new IllegalArgumentException("blank request ID");
        Objects.requireNonNull(command);Objects.requireNonNull(fingerprint);
        if(closed)return new Decision(Kind.CLOSED,0,null,null);
        Committed prior=committed.get(id);
        if(prior!=null)return prior.identity.matches(command,fingerprint)
                ? new Decision(Kind.COMMITTED_DUPLICATE,prior.index,null,null)
                : new Decision(Kind.CONFLICT,0,null,null);
        Active pending=active.get(id);
        if(pending!=null)return pending.identity.matches(command,fingerprint)
                ? new Decision(Kind.JOIN,0,pending.completion,null)
                : new Decision(Kind.CONFLICT,0,null,null);
        Identity uncertain=unresolved.get(id);
        if(uncertain!=null)return new Decision(uncertain.matches(command,fingerprint)
                ? Kind.UNRESOLVED : Kind.CONFLICT,0,null,null);
        Active original=new Active(new Identity(command,fingerprint));
        active.put(id,original);
        return new Decision(Kind.NEW,0,null,new Owner(id,original));
    }
    /** Commit publication and removal of the in-flight owner share one atomic decision boundary. */
    public synchronized Completion finish(Owner owner,Completion result,boolean retainUnresolved) {
        Objects.requireNonNull(owner);Objects.requireNonNull(result);
        Active current=active.get(owner.id);
        if(current!=owner.active)throw new IllegalStateException("request owner changed");
        if(result.code()==CompletionCode.OK && !closed){
            if(committed.putIfAbsent(owner.id,new Committed(current.identity,result.commitIndex()))!=null)
                throw new IllegalStateException("committed record already exists");
        }else if(retainUnresolved){
            unresolved.put(owner.id,current.identity);
        }
        active.remove(owner.id);
        Completion visible=closed ? new Completion(CompletionCode.NOT_READY,0) : result;
        current.completion.complete(visible);
        return visible;
    }
    /** Shutdown wakes joiners but does not infer that a running WAL owner has stopped. */
    public synchronized void closeAdmission() {
        closed=true;
        for(Active value:active.values())value.completion.complete(new Completion(CompletionCode.NOT_READY,0));
    }
    public synchronized int committedCount(){return committed.size();}
    public synchronized int activeCount(){return active.size();}
    public static Completion await(CompletableFuture<Completion> future,long millis) {
        Objects.requireNonNull(future);
        try {return future.get(Math.max(1,millis),TimeUnit.MILLISECONDS);}
        catch(InterruptedException e){Thread.currentThread().interrupt();return new Completion(CompletionCode.NOT_READY,0);}
        catch(ExecutionException | TimeoutException e){return new Completion(CompletionCode.NOT_READY,0);}
    }
}
