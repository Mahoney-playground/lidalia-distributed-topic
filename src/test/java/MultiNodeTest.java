import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Uninterruptibles;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsCollectionContaining.hasItems;
import static org.junit.Assert.assertThat;
import static uk.org.lidalia.lang.Exceptions.throwUnchecked;

public class MultiNodeTest {

    @Test
    public void eventuallyConsistent() throws Exception {
        final AtomicInteger dataToStore = new AtomicInteger(0);

        final int numberOfNodes = 4;
        final List<Node<Integer>> nodes = nodes(numberOfNodes);

        final CountDownLatch allProducersReady = new CountDownLatch(1);

        int numberOfProducers = 10;
        final CountDownLatch allProducersDone = new CountDownLatch(numberOfProducers);

        final int numberOfInserts = 10;

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
                            Uninterruptibles.sleepUninterruptibly(random.nextInt(10), TimeUnit.MILLISECONDS);
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
        allProducersDone.await();

        for (final Node<Integer> node : nodes) {
            waitUntil(new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    return node.synced();
                }
            });
        }

        for (Node<Integer> node : nodes) {
            final ImmutableList<Integer> records = node.records();
            assertThat(records, hasItems(list(1, numberOfProducers * numberOfInserts)));
        }
    }

    private void waitUntil(Callable<Boolean> condition) throws Exception {
        while (!condition.call()) {
            Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
        }
    }

    private List<Node<Integer>> nodes(int numberOfNodes) {
        List<Node<Integer>> nodes = new ArrayList<>();
        for (int i = 1; i <= numberOfNodes; i++) {
            nodes.add(new Node<Integer>(i));
        }
        for (Node<Integer> node : nodes) {
            for (Node<Integer> otherNode : nodes) {
                if (otherNode != node) {
                    node.syncWith(otherNode);
                }
            }
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
}
