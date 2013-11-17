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

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Uninterruptibles;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsCollectionContaining.hasItems;
import static org.junit.Assert.assertThat;
import static uk.org.lidalia.lang.Exceptions.throwUnchecked;

public class ReadingFromMultipleTopicNodesTest {

    @Test
    public void eventuallyConsistent() throws Exception {
        final AtomicInteger dataToStore = new AtomicInteger(0);

        final int numberOfNodes = 4;
        final List<TopicNode> nodes = nodes(numberOfNodes);

        final CountDownLatch allProducersReady = new CountDownLatch(1);

        final int numberOfProducers = 4;
        final CountDownLatch allProducersDone = new CountDownLatch(numberOfProducers);

        final int numberOfInserts = 15;

        for (int i = 1; i <= numberOfProducers; i++) {
            ExecutorService executor = Executors.newSingleThreadExecutor();

            executor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        allProducersReady.await();
                        final Random random = new Random();
                        for (int j = 1; j <= numberOfInserts; j++) {
                            nodes.get(random.nextInt(numberOfNodes)).store(dataToStore.incrementAndGet());
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
        final FeedConsumer feedConsumer = new FeedConsumer(nodes);
        feedConsumer.start();
        allProducersReady.countDown();
        allProducersDone.await();

        waitUntil(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return feedConsumer.getConsumed().size() >= numberOfProducers * numberOfInserts;
            }
        });

        assertThat(feedConsumer.getConsumed(), hasItems(list(1, numberOfProducers * numberOfInserts)));
        assertThat(feedConsumer.getConsumed().size(), is(numberOfProducers * numberOfInserts));
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
        private volatile VectorClock latestRead;
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
                    final ImmutableList<Message> messages = node.consistentMessages();
                    System.out.println("messages: " + messages);
                    if (!messages.isEmpty()) {
                        int placeToStart = messages.size() - 1;
                        while (placeToStart >= 0 && !messages.get(placeToStart).getVectorClock().equals(latestRead)) {
                            placeToStart--;
                        }
                        placeToStart = placeToStart + 1;
                        System.out.println("placeToStart: " + placeToStart);
                        for (int i = placeToStart; i < messages.size(); i++) {
                            final Message message = messages.get(i);
                            consumed.add((Integer) message.get());
                            latestRead = message.getVectorClock();
                        }
                    }
                    System.out.println(consumed);
                    System.out.println();
                }
            }, 0, 1, TimeUnit.SECONDS);
        }

        public List<Integer> getConsumed() {
            return ImmutableList.copyOf(consumed);
        }
    }
}
