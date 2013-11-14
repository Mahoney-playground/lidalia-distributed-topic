package uk.org.lidalia.distributedtopic;

import java.util.Random;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;

public class Synchroniser {

    private final Random random = new Random();
    private final LinkedBlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<>();
    private final ExecutorService syncer = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, workQueue);
    private final Set<TopicNode> otherNodes = new CopyOnWriteArraySet<>();

    public Synchroniser() {
    }

    public void synchronise(final Message message) {
        for (final TopicNode node : otherNodes) {
            syncer.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        Uninterruptibles.sleepUninterruptibly(random.nextInt(100), TimeUnit.MILLISECONDS);
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
