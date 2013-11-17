package uk.org.lidalia.distributedtopic;

import java.util.Map;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSortedMap;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Maps.immutableEntry;
import static uk.org.lidalia.distributedtopic.Maps2.put;
import static uk.org.lidalia.distributedtopic.Maps2.uniqueIndex;

public class DistributedVectorClock {

    private final NodeId nodeId;
    private final ImmutableSortedMap<NodeId, VectorClock> state;

    public DistributedVectorClock(NodeId nodeId) {
        this(nodeId, ImmutableSortedMap.of(nodeId, new VectorClock(nodeId)));
    }

    DistributedVectorClock(NodeId nodeId, ImmutableSortedMap<NodeId, VectorClock> state) {
        this.nodeId = checkNotNull(nodeId);
        this.state = checkNotNull(state);
    }

    public NodeId getNodeId() {
        return nodeId;
    }

    public ImmutableSortedMap<NodeId, VectorClock> getState() {
        return state;
    }

    public VectorClock getLocalClock() {
        return state.get(nodeId);
    }

    public SingleNodeVectorClock getLowestCommonClock() {
        final ImmutableSortedMap<NodeId, SingleNodeVectorClock> lowestCommonClocks = uniqueIndex(state.values(), new Function<VectorClock, Map.Entry<NodeId, SingleNodeVectorClock>>() {
            @Override
            public Map.Entry<NodeId, SingleNodeVectorClock> apply(VectorClock input) {
                return immutableEntry(input.getNodeId(), input.getLowestCommonClock());
            }
        });
        return new VectorClock(nodeId, lowestCommonClocks).getLowestCommonClock();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DistributedVectorClock that = (DistributedVectorClock) o;

        if (!nodeId.equals(that.nodeId)) return false;
        if (!state.equals(that.state)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = nodeId.hashCode();
        result = 31 * result + state.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "{"+nodeId+","+state+'}';
    }

    public DistributedVectorClock update(VectorClock incomingRemoteClock) {
        final ImmutableSortedMap<NodeId, VectorClock> updatedState1 = put(state, incomingRemoteClock.getNodeId(), incomingRemoteClock);
        final ImmutableSortedMap<NodeId, VectorClock> updatedState2 = put(updatedState1, nodeId, getLocalClock().update(incomingRemoteClock.getLocalClock()));
        return new DistributedVectorClock(nodeId, updatedState2);
    }

    public DistributedVectorClock next() {
        final ImmutableSortedMap<NodeId, VectorClock> updatedState = put(state, nodeId, getLocalClock().next());
        return new DistributedVectorClock(nodeId, updatedState);
    }

    public DistributedVectorClock add(NodeId otherNode) {
        return update(new VectorClock(otherNode));
    }
}
