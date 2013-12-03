package uk.org.lidalia.distributedtopic;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Uninterruptibles;

import static com.google.common.collect.FluentIterable.from;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.IsCollectionContaining.hasItems;
import static org.junit.Assert.assertThat;
import static uk.org.lidalia.lang.Exceptions.throwUnchecked;

public class ReadingFromMultipleTopicNodesTest {

    @Test
    public void eventuallyConsistent() throws Exception {
        System.out.println("START!");
        final AtomicInteger dataToStore = new AtomicInteger(0);

        final int numberOfNodes = 9;
        final List<TopicNode> nodes = nodes(numberOfNodes);

        final CountDownLatch allProducersReady = new CountDownLatch(1);

        final int numberOfProducers = 10;
        final CountDownLatch allProducersDone = new CountDownLatch(numberOfProducers);

        final int numberOfInserts = 100;

        for (int i = 1; i <= numberOfProducers; i++) {
            ExecutorService executor = Executors.newSingleThreadExecutor();

            executor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        allProducersReady.await();
                        final Random random = new Random();
                        for (int j = 1; j <= numberOfInserts; j++) {
                            final int message = dataToStore.incrementAndGet();
                            nodes.get(random.nextInt(numberOfNodes)).store(message);
                            if (message % 100 == 0) {
                                System.out.println("Stored "+message);
                            }
                            Uninterruptibles.sleepUninterruptibly(random.nextInt(100), TimeUnit.MILLISECONDS);
                        }
                    } catch (InterruptedException e) {
                        throwUnchecked(e);
                    } finally {
                        allProducersDone.countDown();
                    }
                }
            });
        }
        allProducersReady.countDown();
        final FeedConsumer feedConsumer = new FeedConsumer(nodes);
        feedConsumer.start();
        allProducersDone.await();

        waitUntil(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return feedConsumer.isStopped() || feedConsumer.getConsumed().size() >= numberOfProducers * numberOfInserts || Iterables.getLast(feedConsumer.getConsumed()) == numberOfProducers * numberOfInserts;
            }
        });

        assertThat("Got:"+feedConsumer.getConsumed(), feedConsumer.getConsumed(), hasSize(numberOfProducers * numberOfInserts));
        assertThat(feedConsumer.getConsumed(), hasItems(list(1, numberOfProducers * numberOfInserts)));
    }

    private void waitUntil(Callable<Boolean> condition) throws Exception {
        while (!condition.call()) {
            Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
        }
    }

    private List<TopicNode> nodes(int numberOfNodes) {
        List<TopicNode> nodes = new ArrayList<>();
        for (int i = 1; i <= numberOfNodes; i++) {
            nodes.add(new TopicNode(i));
        }
        for (TopicNode node : nodes) {
            for (TopicNode otherNode : nodes) {
                if (otherNode != node) {
                    node.syncWith(otherNode);
                }
            }
        }
        for (TopicNode node : nodes) {
            node.start();
        }
        return ImmutableList.copyOf(nodes);
    }

    public static Integer[] list(final int start, final int end) {
        Integer[] result = new Integer[(end - start)+1];
        for (int i = start; i <= end; i++) {
            result[i - start] = i;
        }
        return result;
    }

    private static class FeedConsumer {
        private final Random random = new Random();
        private final List<TopicNode> nodes;
        private volatile Optional<SingleNodeVectorClock> latestRead = Optional.absent();
        private final List<Integer> consumed = new CopyOnWriteArrayList<>();

        private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

        private FeedConsumer(List<TopicNode> nodes) {
            this.nodes = nodes;
        }

        public void start() {
            executor.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    final TopicNode node = nodes.get(random.nextInt(nodes.size()));
                    final FluentIterable<Message> messages;
                    if (latestRead.isPresent()) {
                        System.out.println("Getting with latestRead="+latestRead);
                        messages = from(node.consistentMessagesSince(latestRead.get()));
                    } else {
                        System.out.println("Getting no latestRead");
                        messages = from(node.consistentMessages());
                    }
                    consumed.addAll(messages.transform(new Function<Message, Integer>() {
                        @Override
                        public Integer apply(Message input) {
                            return (Integer) input.get();
                        }
                    }).toList());
                    latestRead = messages.last().transform(new Function<Message, SingleNodeVectorClock>() {
                        @Override
                        public SingleNodeVectorClock apply(Message input) {
                            return input.getVectorClock();
                        }
                    }).or(latestRead);
                    System.out.println("Got " + consumed);
                    if (containsDuplicates()) {
                        System.out.println("Inconsistent - stopping");
                        System.out.println("Latest read: "+latestRead);
                        System.out.println("Messages: "+messages);
                        System.out.println("Consumed: "+consumed);
                        stop();
                    }
                    System.out.println();
                }

                private boolean containsDuplicates() {
                    return ImmutableSet.copyOf(consumed).size() != consumed.size();
                }
            }, 0, 10, TimeUnit.MILLISECONDS);
        }

        private void stop() {
            executor.shutdownNow();
        }

        public boolean isStopped() {
            return executor.isShutdown();
        }

        public List<Integer> getConsumed() {
            return ImmutableList.copyOf(consumed);
        }
    }
}
