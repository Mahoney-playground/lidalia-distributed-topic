package uk.org.lidalia.distributedtopic;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;
import org.joda.time.Instant;

import java.util.Map;

import static com.google.common.base.Optional.fromNullable;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.FluentIterable.from;
import static com.google.common.collect.Maps.immutableEntry;
import static org.joda.time.Instant.now;
import static uk.org.lidalia.distributedtopic.Maps2.put;
import static uk.org.lidalia.distributedtopic.Maps2.uniqueIndex;

public class SingleNodeVectorClock implements Comparable<SingleNodeVectorClock> {

    private final NodeId nodeId;
    private final ImmutableSortedMap<NodeId, Integer> state;
    private final Instant timestamp = now();

    public SingleNodeVectorClock(NodeId nodeId, NodeId... nodeIds) {
        this(nodeId, ImmutableSortedSet.<NodeId>naturalOrder().add(nodeId).add(nodeIds).build());
    }

    public SingleNodeVectorClock(NodeId nodeId, ImmutableSet<NodeId> nodeIds) {
        this(nodeId, initialStateFor(nodeId, nodeIds));
    }

    private static ImmutableSortedMap<NodeId, Integer> initialStateFor(NodeId nodeId, ImmutableSet<NodeId> nodeIds) {
        ImmutableSortedMap<NodeId, Integer> initialState = uniqueIndex(nodeIds, new Function<NodeId, Map.Entry<NodeId, Integer>>() {
            @Override
            public Map.Entry<NodeId, Integer> apply(NodeId nodeId) {
                return immutableEntry(nodeId, 0);
            }
        });
        return put(initialState, nodeId, 1);
    }

    SingleNodeVectorClock(NodeId nodeId, ImmutableSortedMap<NodeId, Integer> state) {
        this.nodeId = checkNotNull(nodeId);
        this.state = checkNotNull(state);
    }

    public NodeId getNodeId() {
        return nodeId;
    }

    ImmutableSortedMap<NodeId, Integer> getState() {
        return state;
    }

    public SingleNodeVectorClock next() {
        return new SingleNodeVectorClock(nodeId, incrementedState());
    }

    public SingleNodeVectorClock update(NodeId otherNodeId, Integer otherNodeSequence) {
        return new SingleNodeVectorClock(nodeId, put(incrementedState(), otherNodeId, otherNodeSequence));
    }

    private ImmutableSortedMap<NodeId, Integer> incrementedState() {
        return put(state, nodeId, sequenceForDefiningNode() + 1);
    }

    public Integer sequenceForDefiningNode() {
        return sequenceFor(nodeId).get();
    }

    public Optional<Integer> sequenceFor(NodeId nodeId) {
        return Optional.fromNullable(state.get(nodeId));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SingleNodeVectorClock that = (SingleNodeVectorClock) o;

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

    @Override
    public int compareTo(SingleNodeVectorClock other) {
        if (equals(other)) {
            return 0;
        } else if (nodeId.equals(other.nodeId)) {
            return sequenceForDefiningNode() - other.sequenceForDefiningNode();
        } else if (isBefore(other)) {
            return  -1;
        } else if (isAfter(other)) {
            return 1;
        } else  {
            int timestampdiff = timestamp.compareTo(other.timestamp);
            if (timestampdiff != 0) {
                return timestampdiff;
            } else {
                int diff = sequenceDiff(other);
                if (diff != 0) {
                    return diff;
                } else {
                    return orderedSequenceCompare(other);
                }
            }
        }
    }

    private int orderedSequenceCompare(SingleNodeVectorClock other) {
        for (NodeId nodeId : nodeIds()) {
            int diff = sequenceFor(nodeId).get() - other.sequenceFor(nodeId).or(0);
            if (diff != 0) {
                return diff;
            }
        }
        return -1;
    }

    private int sequenceDiff(SingleNodeVectorClock other) {
        int sequenceTotal = total(state.values());
        int otherSequenceTotal = total(other.state.values());
        return sequenceTotal - otherSequenceTotal;
    }

    private int total(ImmutableCollection<Integer> values) {
        int acc = 0;
        for (Integer value : values) {
            acc += value;
        }
        return acc;
    }

    private boolean isAfter(final SingleNodeVectorClock other) {
        return haveSameNodeSet(other) && from(nodeIds()).allMatch(new Predicate<NodeId>() {
            @Override
            public boolean apply(NodeId nodeId) {
                return sequenceFor(nodeId).get() >= other.sequenceFor(nodeId).get();
            }
        });
    }

    private boolean isBefore(final SingleNodeVectorClock other) {
        return haveSameNodeSet(other) && from(nodeIds()).allMatch(new Predicate<NodeId>() {
            @Override
            public boolean apply(NodeId nodeId) {
                return sequenceFor(nodeId).get() <= other.sequenceFor(nodeId).get();
            }
        });
    }

    private boolean haveSameNodeSet(SingleNodeVectorClock other) {
        return nodeIds().equals(other.nodeIds());
    }

    public ImmutableSortedSet<NodeId> nodeIds() {
        return state.keySet();
    }
}
