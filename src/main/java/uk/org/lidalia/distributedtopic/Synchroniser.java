package uk.org.lidalia.distributedtopic;

import java.util.Random;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.joda.time.Duration;
import org.joda.time.Instant;

import com.google.common.util.concurrent.Uninterruptibles;

import static org.joda.time.Instant.now;

public class Synchroniser {

    private final Random random = new Random();
    private final LinkedBlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<>();
    private final ExecutorService syncer = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, workQueue);
    private final Set<TopicNode> otherNodes = new CopyOnWriteArraySet<>();

    public Synchroniser() {
    }

    public void synchronise(final Message message) {
        for (final TopicNode node : otherNodes) {
            final Instant submissionTime = now();
            syncer.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        final Duration latency = Duration.millis(random.nextInt(1000) + 100);
                        final Instant arrivalTime = submissionTime.plus(latency);
                        final Duration timeToWait = new Duration(now(), arrivalTime);
                        if (timeToWait.getMillis() > 0) {
                            Uninterruptibles.sleepUninterruptibly(timeToWait.getMillis(), TimeUnit.MILLISECONDS);
                        }
                        node.sync(message);
                    } catch (Throwable t) {
                        t.printStackTrace();
                    }
                }
            });
        }
    }

    public void syncWith(TopicNode otherNode) {
        otherNodes.add(otherNode);
    }

    public int queue() {
        return workQueue.size();
    }
}
