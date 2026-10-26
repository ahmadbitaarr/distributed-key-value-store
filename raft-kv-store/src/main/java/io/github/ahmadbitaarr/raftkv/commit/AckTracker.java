package io.github.ahmadbitaarr.raftkv.commit;

import io.github.ahmadbitaarr.raftkv.config.ClusterConfig;
import io.github.ahmadbitaarr.raftkv.log.LogEntry;
import io.github.ahmadbitaarr.raftkv.log.WalBackedPersistentLog;
import io.github.ahmadbitaarr.raftkv.replication.FollowerClient;
import io.github.ahmadbitaarr.raftkv.replication.FollowerStatusProbe;
import io.github.ahmadbitaarr.raftkv.server.NodeRole;
import io.github.ahmadbitaarr.raftkv.wal.WalStore;
import java.util.*;

/** Conservative, process-local matching-prefix proof; never treats high-water marks as certificates. */
public final class AckTracker {
    private final ClusterConfig config;
    private final WalBackedPersistentLog log;
    private final WalStore wal;
    private final Map<String, Long> verified = new HashMap<>(); // -1 = unknown
    private final Map<Long, MajorityCertificate> issued = new HashMap<>();

    public AckTracker(ClusterConfig config, WalBackedPersistentLog log, WalStore wal) {
        this.config=Objects.requireNonNull(config); this.log=Objects.requireNonNull(log);this.wal=Objects.requireNonNull(wal);
        if(config.localRole()!=NodeRole.LEADER) throw new IllegalArgumentException("leader only");
        for(String member:config.members().keySet())
            if(!member.equals(config.configuredLeaderId())) verified.put(member,-1L);
    }

    /** Empty, valid, identified FOLLOWER status alone can bootstrap prefix zero. */
    public synchronized boolean certifyEmpty(String target, FollowerStatusProbe.Status status) {
        requireFollower(target);
        if(verified.get(target)>=0) return false;
        if(status==null || !target.equals(status.nodeId()) || status.role()!=NodeRole.FOLLOWER
                || status.lastLogIndex()!=0 || status.commitIndex()!=0 || status.appliedIndex()!=0) return false;
        verified.put(target,0L);
        return true;
    }

    /** Nonempty status certifies only the mathematically empty prefix, never its reported entries. */
    public synchronized boolean beginNonemptyVerification(String target, FollowerStatusProbe.Status status) {
        requireFollower(target);
        if(status==null || !target.equals(status.nodeId()) || status.role()!=NodeRole.FOLLOWER
                || status.lastLogIndex()<=0 || status.commitIndex()>status.lastLogIndex()
                || status.appliedIndex()>status.commitIndex()) return false;
        invalidate(target);
        verified.put(target,0L);
        return true;
    }

    /** Invalidates all certificates relying on this follower's prior evidence. */
    public synchronized void invalidate(String target) {
        requireFollower(target);
        verified.put(target,-1L);
        issued.entrySet().removeIf(e -> e.getValue().voters().contains(target));
    }

    public synchronized long verifiedMatchingThrough(String target) {requireFollower(target);return verified.get(target);}

    /** A validated outcome binds the exact requested leader entry to the next consecutive index. */
    public synchronized boolean recordDurableAck(String follower, LogEntry entry, FollowerClient.Outcome ack) {
        requireFollower(follower);
        if(entry==null || ack==null || ack.kind()!=FollowerClient.Kind.ACK
                || ack.requestedIndex()!=entry.index() || ack.lastReportedPersistedIndex()<entry.index()
                || entry.index()!=verified.get(follower)+1 || wal.lastDurableLogIndex()<entry.index()
                || !log.get(entry.index()).filter(entry::equals).isPresent()) return false;
        verified.put(follower,entry.index());
        return true;
    }

    public synchronized Optional<MajorityCertificate> majorityFor(LogEntry entry) {
        if(entry==null || wal.lastDurableLogIndex()<entry.index()
                || !log.get(entry.index()).filter(entry::equals).isPresent()) return Optional.empty();
        for(String follower:new TreeSet<>(verified.keySet())) {
            if(verified.get(follower)>=entry.index()) {
                return Optional.of(issued.computeIfAbsent(entry.index(), ignored ->
                        new MajorityCertificate(entry,Set.of(config.localNodeId(),follower))));
            }
        }
        return Optional.empty();
    }

    /** Prevent forged/reused certificate instances or certificates for a different entry. */
    public synchronized boolean validates(MajorityCertificate certificate, LogEntry entry) {
        if(certificate==null || entry==null || issued.get(entry.index())!=certificate
                || !certificate.entry.equals(entry) || wal.lastDurableLogIndex()<entry.index()
                || !log.get(entry.index()).filter(entry::equals).isPresent()) return false;
        if(!certificate.voters.contains(config.localNodeId()) || certificate.voters.size()!=2) return false;
        for(String follower:certificate.voters) {
            if(!follower.equals(config.localNodeId()) && verified.getOrDefault(follower,-1L)<entry.index())return false;
        }
        return true;
    }

    private void requireFollower(String id) {
        if(id==null || !verified.containsKey(id)) throw new IllegalArgumentException("unknown follower identity");
    }

    /** Unforgeable by ordinary callers: created only by an AckTracker with verified evidence. */
    public static final class MajorityCertificate {
        private final LogEntry entry;
        private final Set<String> voters;
        private MajorityCertificate(LogEntry entry, Set<String> voters) {
            this.entry=entry;this.voters=Set.copyOf(voters);
        }
        public LogEntry entry(){return entry;}
        public Set<String> voters(){return voters;}
        public long index(){return entry.index();}
    }
}
