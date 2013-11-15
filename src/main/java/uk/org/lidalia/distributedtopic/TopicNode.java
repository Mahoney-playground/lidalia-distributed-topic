package uk.org.lidalia.distributedtopic;

import java.util.Comparator;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedSet;

import static uk.org.lidalia.distributedtopic.FluentIterable2.from;

public class TopicNode {

    private static final Object heartBeat = "HEARTBEAT";

    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    private final ConcurrentSkipListSet<Message> messages = new ConcurrentSkipListSet<>();
    private volatile VectorClock vectorClock;

    private final NodeId id;

    private final Object lock = new Object();
    private final Synchroniser synchroniser = new Synchroniser();

    public TopicNode(final int id) {
        this.id = new NodeId(id);
        this.vectorClock = new VectorClock(this.id);
    }

    public void start() {
        executor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                synchronized (lock) {
                    storeMessage(heartBeat);
                }
            }
        }, 0, 100, TimeUnit.MILLISECONDS);
    }

    public void syncWith(TopicNode otherNode) {
        vectorClock = vectorClock.add(otherNode.id);
        synchroniser.syncWith(otherNode);
    }

    public void store(final Object value) {
        synchronized (lock) {
            storeMessage(value);
        }
    }

    private void storeMessage(Object value) {
        vectorClock = vectorClock.next();
        final Message message = new Message(value, vectorClock.getLocalClock());
        messages.add(message);
        synchroniser.synchronise(message);
    }

    public void sync(Message message) {
        synchronized (lock) {
            messages.add(message);
            vectorClock = vectorClock.update(message.getVectorClock());
        }
    }

    public ImmutableList<Message> consistentMessages() {
        final ImmutableSortedSet<Message> messageSnapshot;
        final VectorClock vectorClock;
        synchronized (lock) {
            messageSnapshot = ImmutableSortedSet.copyOf(messages);
            vectorClock = this.vectorClock;
        }
        final SingleNodeVectorClock lowestCommonClock = vectorClock.getLowestCommonClock();
        System.out.println("all messages: "+messageSnapshot);
        System.out.println("vectorClock: "+vectorClock);
        System.out.println("lowestCommonClock: "+lowestCommonClock);
        return from(messageSnapshot).filter(outHeartbeats()).takeWhile(new Predicate<Message>() {
            @Override
            public boolean apply(final Message message) {
                return message.getVectorClock().isBefore(lowestCommonClock);
            }
        }).toList();
    }

    private Comparator<? super Message> sortCriteria(final SingleNodeVectorClock lowestCommonClock) {
        return new Comparator<Message>() {
            @Override
            public int compare(Message message1, Message message2) {
                final SingleNodeVectorClock vectorClock1 = message1.getVectorClock();
                final SingleNodeVectorClock vectorClock2 = message2.getVectorClock();
                if (vectorClock1.equals(vectorClock2)) {
                    return 0;
                } else if (vectorClock1.getNodeId().equals(vectorClock2.getNodeId())) {
                    return vectorClock1.sequenceForDefiningNode() - vectorClock2.sequenceForDefiningNode();
                } else if (vectorClock1.isBefore(vectorClock2)) {
                    return  -1;
                } else if (vectorClock1.isAfter(vectorClock2)) {
                    return 1;
                } else if (vectorClock1.isBefore(lowestCommonClock) && !vectorClock2.isBefore(lowestCommonClock)) {
                    return -1;
                } else if (vectorClock2.isBefore(lowestCommonClock) && !vectorClock1.isBefore(lowestCommonClock)) {
                    return 1;
                } else  {
                    int timestampDiff = vectorClock1.timestamp.compareTo(vectorClock2.timestamp);
                    if (timestampDiff != 0) {
                        return timestampDiff;
                    } else {
                        int diff = sequenceDiff(vectorClock1, vectorClock2);
                        if (diff != 0) {
                            return diff;
                        } else {
                            return orderedSequenceCompare(vectorClock1, vectorClock2);
                        }
                    }
                }
            }
            private int orderedSequenceCompare(SingleNodeVectorClock vectorClock1, SingleNodeVectorClock vectorClock2) {
                for (NodeId nodeId : vectorClock1.nodeIds()) {
                    int diff = vectorClock1.sequenceFor(nodeId).get() - vectorClock2.sequenceFor(nodeId).or(0);
                    if (diff != 0) {
                        return diff;
                    }
                }
                throw new AssertionError("It should be impossible to have two clocks with the same sequences that are not equal");
            }

            private int sequenceDiff(SingleNodeVectorClock vectorClock1, SingleNodeVectorClock vectorClock2) {
                int sequenceTotal = total(vectorClock1.getState().values());
                int otherSequenceTotal = total(vectorClock2.getState().values());
                return sequenceTotal - otherSequenceTotal;
            }

            private int total(ImmutableCollection<Integer> values) {
                int acc = 0;
                for (Integer value : values) {
                    acc += value;
                }
                return acc;
            }
        };
    }

    public ImmutableList<Message> allMessages() {
        return from(ImmutableList.copyOf(messages)).filter(outHeartbeats()).toList();
    }

    private Predicate<Message> outHeartbeats() {
        return new Predicate<Message>() {
            @Override
            public boolean apply(Message message) {
                return !message.get().equals(heartBeat);
            }
        };
    }

    public boolean synced() {
        return synchroniser.queue() == 0;
    }
}
