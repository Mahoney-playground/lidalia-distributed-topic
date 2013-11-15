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
