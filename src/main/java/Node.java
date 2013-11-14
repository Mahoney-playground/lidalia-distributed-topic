import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
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
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Uninterruptibles;

import static com.google.common.collect.FluentIterable.from;

public class Node<T> {

    private final Random random = new Random();

    private final LinkedBlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<>();
    private final ExecutorService syncer = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, workQueue);
    private final AtomicInteger sequence = new AtomicInteger(0);

    private final ConcurrentSkipListSet<Record<T>> records = new ConcurrentSkipListSet<>();
    private final Set<Node<T>> otherNodes = new HashSet<>();
    private final ConcurrentMap<Integer, Integer> sequences = new ConcurrentHashMap<>();
    private final ConcurrentMap<Integer, ConcurrentMap<Integer, Integer>> allSequences = new ConcurrentHashMap<>();

    private final int id;

    private final Object lock = new Object();

    public Node(int id) {
        this.id = id;
        sequences.putIfAbsent(id, 0);
        allSequences.putIfAbsent(id, sequences);
    }

    public void syncWith(Node<T> otherNode) {
        otherNodes.add(otherNode);
        sequences.putIfAbsent(otherNode.id, 0);
    }

    public void store(final T value) {
        final ImmutableMap<Integer, Integer> sequenceSnapshot;
        synchronized (lock) {
            sequences.put(id, sequence.incrementAndGet());
            allSequences.put(id, sequences);
            sequenceSnapshot = ImmutableMap.copyOf(sequences);
        }

        final Record<T> record = new Record<>(value, id, sequenceSnapshot);
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
        synchronized (lock) {
            records.add(record);
            sequences.put(record.getNodeId(), record.getSeqNo().get(record.getNodeId()));
            allSequences.put(id, sequences);
        }
    }

    public ImmutableList<Record<T>> records2() {
        final ImmutableList<Record<T>> recordSnapshot;
        final ImmutableMap<Integer, ImmutableMap<Integer, Integer>> sequencesSnapshot;
        synchronized (lock) {
            recordSnapshot = ImmutableList.copyOf(records);
            sequencesSnapshot = takeSnapshot(allSequences);
        }
        System.out.println("all records: "+recordSnapshot);
        System.out.println("Sequences: "+sequencesSnapshot);
        ImmutableMap<Integer, Integer> commonRecord;



        return from(ImmutableList.copyOf(recordSnapshot)).filter(new Predicate<Record<T>>() {
            @Override
            public boolean apply(final Record<T> record) {
                return from(sequencesSnapshot.keySet()).allMatch(new Predicate<Integer>() {
                    @Override
                    public boolean apply(Integer nodeId) {
                        return sequencesSnapshot.get(nodeId) > record.getSeqNo().get(nodeId);
                    }
                });
            }
        }).toList();
    }

    private ImmutableMap<Integer, ImmutableMap<Integer, Integer>> takeSnapshot(ConcurrentMap<Integer, ConcurrentMap<Integer, Integer>> allSequences) {
        synchronized (lock) {
            Map<Integer, ImmutableMap<Integer, Integer>> acc = new HashMap<>();
            for (Integer nodeId : allSequences.keySet()) {
                acc.put(nodeId, ImmutableMap.copyOf(allSequences.get(nodeId)));
            }
            return ImmutableMap.copyOf(acc);
        }
    }

    public ImmutableList<T> records() {
        return FluentIterable.from(ImmutableList.copyOf(records)).transform(new Function<Record<T>, T>() {
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
