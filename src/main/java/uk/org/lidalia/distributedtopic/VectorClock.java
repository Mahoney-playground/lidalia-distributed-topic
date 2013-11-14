package uk.org.lidalia.distributedtopic;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Maps;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.FluentIterable.from;
import static com.google.common.collect.Maps.immutableEntry;
import static uk.org.lidalia.distributedtopic.Maps2.uniqueIndex;

public class VectorClock implements Comparable<VectorClock> {

    private final NodeId nodeId;
    private final ImmutableSortedMap<NodeId, SingleNodeVectorClock> state;

    public VectorClock(NodeId nodeId, NodeId... nodeIds) {
        this(nodeId, ImmutableSortedSet.<NodeId>naturalOrder().add(nodeId).add(nodeIds).build());
    }

    public VectorClock(NodeId nodeId, ImmutableSortedSet<NodeId> nodeIds) {
        this(nodeId, initialStateFor(nodeIds));
    }

    private static ImmutableSortedMap<NodeId, SingleNodeVectorClock> initialStateFor(final ImmutableSortedSet<NodeId> nodeIds) {
        return uniqueIndex(nodeIds, new Function<NodeId, Map.Entry<NodeId, SingleNodeVectorClock>>() {
            @Override
            public Map.Entry<NodeId, SingleNodeVectorClock> apply(NodeId nodeId) {
                return immutableEntry(nodeId, new SingleNodeVectorClock(nodeId, nodeIds));
            }
        });
    }

    private VectorClock(NodeId nodeId, ImmutableSortedMap<NodeId, SingleNodeVectorClock> state) {
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
        FluentIterable<NodeId> allKeys = from(state.keySet()).transformAndConcat(new Function<NodeId, Iterable<NodeId>>() {
            @Override
            public Iterable<NodeId> apply(NodeId input) {
                return state.get(input).nodeIds();
            }
        });
        

        ImmutableSortedMap<NodeId, Integer> lowestCommonState = Maps2.uniqueIndex(allKeys, new Function<NodeId, Map.Entry<NodeId, Integer>>() {
            @Override
            public Map.Entry<NodeId, Integer> apply(NodeId nodeId) {
                return immutableEntry(nodeId, );
            }
        });
        return new SingleNodeVectorClock(nodeId, lowestCommonState);
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
    public int compareTo(VectorClock o) {
        return 0;
    }

    @Override
    public String toString() {
        return "{"+nodeId+","+state+'}';
    }

    public VectorClock update(SingleNodeVectorClock updatedRemoteClock) {
        Map<NodeId, SingleNodeVectorClock> updatedState = new HashMap<>(state);
        updatedState.put(updatedRemoteClock.getNodeId(), updatedRemoteClock);
        updatedState.put(nodeId, getLocalClock().update(updatedRemoteClock.getNodeId(), updatedRemoteClock.sequenceForDefiningNode()));
        return new VectorClock(nodeId, ImmutableSortedMap.copyOf(updatedState));
    }
}
