import java.util.UUID;

import com.google.common.collect.ImmutableMap;

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
//        final int idDiff = nodeId - o.nodeId;
//        if (idDiff == 0) {
//            return seqNo - o.seqNo;
//        } else {
//            return idDiff;
//        }
        return -1;
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
