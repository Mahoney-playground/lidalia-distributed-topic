package uk.org.lidalia.distributedtopic;

import java.util.Map;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.FluentIterable.from;
import static com.google.common.collect.Maps.immutableEntry;
import static java.util.Collections.min;
import static uk.org.lidalia.distributedtopic.Maps2.put;
import static uk.org.lidalia.distributedtopic.Maps2.uniqueIndex;

public class VectorClock {

    private final NodeId nodeId;
    private final ImmutableSortedMap<NodeId, SingleNodeVectorClock> state;

    public VectorClock(NodeId nodeId) {
        this(nodeId, ImmutableSortedMap.of(nodeId, new SingleNodeVectorClock(nodeId)));
    }

    VectorClock(NodeId nodeId, ImmutableSortedMap<NodeId, SingleNodeVectorClock> state) {
        this.nodeId = checkNotNull(nodeId);
        this.state = checkNotNull(state);
    }

    public NodeId getNodeId() {
        return nodeId;
    }

    public ImmutableSortedMap<NodeId, SingleNodeVectorClock> getState() {
        return state;
    }

    public SingleNodeVectorClock getLocalClock() {
        return state.get(nodeId);
    }

    public SingleNodeVectorClock getLowestCommonClock() {
        ImmutableSet<NodeId> allNodeIds = from(state.values()).transformAndConcat(new Function<SingleNodeVectorClock, Iterable<NodeId>>() {
            @Override
            public Iterable<NodeId> apply(SingleNodeVectorClock input) {
                return input.nodeIds();
            }
        }).toSet();

        ImmutableSortedMap<NodeId, Integer> lowestCommonState = uniqueIndex(allNodeIds, new Function<NodeId, Map.Entry<NodeId, Integer>>() {
            @Override
            public Map.Entry<NodeId, Integer> apply(NodeId nodeId) {
                Integer minimum = minimumValueFor(nodeId);
                return immutableEntry(nodeId, minimum);
            }
        });
        return new SingleNodeVectorClock(nodeId, lowestCommonState);
    }

    private Integer minimumValueFor(final NodeId nodeId) {
        return min(from(state.values()).transform(new Function<SingleNodeVectorClock, Integer>() {
            @Override
            public Integer apply(SingleNodeVectorClock vectorClock) {
                return vectorClock.sequenceFor(nodeId).or(0);
            }
        }).toSet());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        VectorClock that = (VectorClock) o;

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
        return state.toString();
    }

    public VectorClock update(SingleNodeVectorClock updatedRemoteClock) {
        final ImmutableSortedMap<NodeId, SingleNodeVectorClock> updatedState1 = put(state, updatedRemoteClock.getNodeId(), updatedRemoteClock);
        final ImmutableSortedMap<NodeId, SingleNodeVectorClock> updatedState2 = put(updatedState1, nodeId, getLocalClock().update(updatedRemoteClock));
        return new VectorClock(nodeId, updatedState2);
    }

    public VectorClock next() {
        final ImmutableSortedMap<NodeId, SingleNodeVectorClock> updatedState = put(state, nodeId, getLocalClock().next());
        return new VectorClock(nodeId, updatedState);
    }

    public VectorClock add(NodeId otherNode) {
        return update(new SingleNodeVectorClock(otherNode));
    }
}
