package uk.org.lidalia.distributedtopic;

import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedSet;

import static uk.org.lidalia.distributedtopic.FluentIterable2.from;

public class TopicNode {

    private static final Object heartBeat = "HEARTBEAT";

    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    private final ConcurrentSkipListSet<Message> messages = new ConcurrentSkipListSet<>();
    private volatile DistributedVectorClock vectorClock;

    private final NodeId id;

    private final Synchroniser synchroniser = new Synchroniser();
    private final AtomicBoolean needsHeartbeat = new AtomicBoolean(false);

    public TopicNode(final int id) {
        this.id = new NodeId(id);
        this.vectorClock = new DistributedVectorClock(this.id);
    }

    public void start() {
        executor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                if (synchroniser.queue() < (vectorClock.getState().size() * 2) && needsHeartbeat.getAndSet(false)) {
                    store(heartBeat);
                }
            }
        }, 0, 100, TimeUnit.MILLISECONDS);
    }

    public void syncWith(TopicNode otherNode) {
        vectorClock = vectorClock.add(otherNode.id);
        synchroniser.syncWith(otherNode);
    }

    public synchronized void store(final Object value) {
        vectorClock = vectorClock.next();
        final Message message = new Message(value, vectorClock.getLocalClock());
        messages.add(message);
        synchroniser.synchronise(message);
    }

    public synchronized void sync(Message message) {
        vectorClock = vectorClock.update(message.getVectorClock());
        messages.add(message);
        needsHeartbeat.set(true);
    }

    public synchronized ImmutableList<Message> consistentMessagesSince(SingleNodeVectorClock vectorClock1) {
        SingleNodeVectorClock lowestCommonClock = vectorClock.getLowestCommonClock();
        final ImmutableSortedSet<Message> messageSnapshot = ImmutableSortedSet.copyOf(messages);
        return from(messageSnapshot).takeWhile(before(ImmutableSortedSet.of(lowestCommonClock, vectorClock1).last())).filter(heartbeats()).toList();
    }

    public synchronized ImmutableList<Message> consistentMessages() {
        return consistentMessagesSince(vectorClock.getLowestCommonClock());
    }

    private Predicate<Message> before(final SingleNodeVectorClock lowestCommonClock) {
        return new Predicate<Message>() {
            @Override
            public boolean apply(final Message message) {
                return message.getVectorClock().getLocalClock().isBefore(lowestCommonClock);
            }
        };
    }

    public ImmutableList<Message> allMessages() {
        return from(ImmutableList.copyOf(messages)).filter(heartbeats()).toList();
    }

    private Predicate<Message> heartbeats() {
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
