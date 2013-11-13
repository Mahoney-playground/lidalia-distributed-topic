import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Uninterruptibles;

public class Node<T> {

    private final Random random = new Random();

    private final LinkedBlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<>();
    private final ExecutorService syncer = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, workQueue);
    private final AtomicInteger sequence = new AtomicInteger(0);

    private final ConcurrentSkipListSet<Record<T>> records = new ConcurrentSkipListSet<>();
    private final Set<Node<T>> otherNodes = new HashSet<>();
    private final ConcurrentMap<Integer, Integer> sequences = new ConcurrentHashMap<>();

    private final int id;

    public Node(int id) {
        this.id = id;
    }

    public void syncWith(Node<T> otherNode) {
        otherNodes.add(otherNode);
    }

    public void store(T value) {
        final Record<T> record = new Record<>(value, id, sequence.getAndIncrement());
        records.add(record);
        for (final Node<T> node : otherNodes) {
            syncer.submit(new Runnable() {
                @Override
                public void run() {
                    Uninterruptibles.sleepUninterruptibly(random.nextInt(100), TimeUnit.MILLISECONDS);
                    node.sync(record);
                }
            });
        }
    }

    public void sync(Record<T> record) {
        records.add(record);
        sequences.put(record.getNodeId(), record.getSeqNo());
    }

    public ImmutableList<Record<T>> records2() {
        return ImmutableList.copyOf(records);
    }

    public ImmutableList<T> records() {
        return FluentIterable.from(records).transform(new Function<Record<T>, T>() {
            @Override
            public T apply(Record<T> record) {
                return record.get();
            }
        }).toList();
    }

    public boolean synced() {
        return workQueue.size() == 0;
    }
}
