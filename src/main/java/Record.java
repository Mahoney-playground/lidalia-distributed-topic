import java.util.TreeSet;
import java.util.UUID;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableMap;

import static com.google.common.collect.FluentIterable.from;

public class Record<T> implements Comparable<Record<T>> {

    private final UUID id = UUID.randomUUID();
    private final T value;
    private final int nodeId;
    private final ImmutableMap<Integer, Integer> seqNo;

    public Record(T value, int nodeId, ImmutableMap<Integer, Integer> seqNo) {
        this.value = value;
        this.nodeId = nodeId;
        this.seqNo = seqNo;
    }

    public T get() {
        return value;
    }

    @Override
    public int compareTo(Record<T> o) {
        if (o.id.equals(id)) {
            return 0;
        } else if (o.getNodeId() == getNodeId()) {
            return seqNo.get(nodeId) - o.getSeqNo().get(nodeId);
        } else if (happenedBefore(o)) {
            return -1;
        } else if (happenedAfter(o)) {
            return 1;
        } else {
            final int diff = total(seqNo.values()) - total(o.seqNo.values());
            if (diff != 0) {
                return diff;
            } else {
                return firstSeqNoDifferentIn(o);
            }
        }
    }

    private int firstSeqNoDifferentIn(Record<T> o) {
        for (Integer nodeId : new TreeSet<>(seqNo.keySet())) {
            int diff = seqNo.get(nodeId) - o.seqNo.get(nodeId);
            if (diff != 0) {
                return diff;
            }
        }
        throw new IllegalStateException("Should be impossible to have two records with the same sequence values! "+this+" and "+o);
    }

    private int total(ImmutableCollection<Integer> values) {
        int acc = 0;
        for (Integer i : values) {
            acc += i;
        }
        return acc;
    }

    private boolean happenedBefore(final Record<T> o) {
        return from(seqNo.keySet()).allMatch(new Predicate<Integer>() {
                @Override
                public boolean apply(Integer nodeId) {
                    return seqNo.get(nodeId) < o.getSeqNo().get(nodeId);
                }
            });
    }

    private boolean happenedAfter(final Record<T> o) {
        return from(seqNo.keySet()).allMatch(new Predicate<Integer>() {
            @Override
            public boolean apply(Integer nodeId) {
                return seqNo.get(nodeId) > o.getSeqNo().get(nodeId);
            }
        });
    }

    public UUID getId() {
        return id;
    }

    public int getNodeId() {
        return nodeId;
    }

    public ImmutableMap<Integer, Integer> getSeqNo() {
        return seqNo;
    }

    @Override
    public String toString() {
        return "Record{" +
                "id=" + id +
                ", value=" + value +
                ", nodeId=" + nodeId +
                ", seqNo=" + seqNo +
                '}';
    }
}
