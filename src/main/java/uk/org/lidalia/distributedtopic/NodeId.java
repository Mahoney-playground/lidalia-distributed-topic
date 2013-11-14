package uk.org.lidalia.distributedtopic;

import uk.org.lidalia.lang.WrappedValue;

public class NodeId extends WrappedValue implements Comparable<NodeId> {

    private final Integer id;

    public NodeId(Integer id) {
        super(id);
        this.id = id;
    }

    public Integer getId() {
        return id;
    }

    @Override
    public int compareTo(NodeId o) {
        return id - o.id;
    }

    @Override
    public String toString() {
        return "node["+id+']';
    }
}
