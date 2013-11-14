package uk.org.lidalia.distributedtopic;

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
import java.util.concurrent.ScheduledExecutorService;
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
    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    private final AtomicInteger sequence = new AtomicInteger(0);

    private final ConcurrentSkipListSet<Record<T>> records = new ConcurrentSkipListSet<>();
    private final Set<Node<T>> otherNodes = new HashSet<>();
    private final ConcurrentMap<Integer, Integer> sequences = new ConcurrentHashMap<>();
    private final ConcurrentMap<Integer, ImmutableMap<Integer, Integer>> allSequences = new ConcurrentHashMap<>();

    private final int id;

    private final Object lock = new Object();

    public Node(final int id) {
        this.id = id;
        sequences.putIfAbsent(id, 0);
        allSequences.putIfAbsent(id, ImmutableMap.copyOf(sequences));
    }

    public void start() {
        executor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                synchronized (lock) {
                    sequences.put(id, sequence.incrementAndGet());
                    allSequences.put(id, ImmutableMap.copyOf(sequences));
                    final ImmutableMap<Integer, Integer> sequenceSnapshot = ImmutableMap.copyOf(sequences);
                    for (final Node<T> node : otherNodes) {
                        syncer.submit(new Runnable() {
                            @Override
                            public void run() {
                                try {
                                    Uninterruptibles.sleepUninterruptibly(random.nextInt(100), TimeUnit.MILLISECONDS);
                                    node.heartbeat(id, ImmutableMap.copyOf(sequenceSnapshot));
                                } catch (Throwable t) {
                                    t.printStackTrace();
                                }
                            }
                        });
                    }
                }
            }
        }, 0, 10, TimeUnit.MILLISECONDS);
    }

    public void syncWith(Node<T> otherNode) {
        otherNodes.add(otherNode);
        sequences.putIfAbsent(otherNode.id, 0);
        allSequences.putIfAbsent(otherNode.id, initialSequenceMap());
    }

    private ImmutableMap<Integer, Integer> initialSequenceMap() {
        Map<Integer, Integer> sequenceMap = new HashMap<>();
        for (Integer nodeId: nodeIds()) {
            sequenceMap.put(nodeId, -1);
        }
        return ImmutableMap.copyOf(sequenceMap);
    }

    public void store(final T value) {
        final ImmutableMap<Integer, Integer> sequenceSnapshot;
        synchronized (lock) {
            sequences.put(id, sequence.incrementAndGet());
            allSequences.put(id, ImmutableMap.copyOf(sequences));
            sequenceSnapshot = ImmutableMap.copyOf(sequences);

            final Record<T> record = new Record<>(value, id, sequenceSnapshot);
            records.add(record);
            for (final Node<T> node : otherNodes) {
                syncer.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            Uninterruptibles.sleepUninterruptibly(random.nextInt(100), TimeUnit.MILLISECONDS);
                            node.sync(record);
                        } catch (Throwable t) {
                            t.printStackTrace();
                        }
                    }
                });
            }
        }
    }

    public void sync(Record<T> record) {
        synchronized (lock) {
            records.add(record);
            sequences.put(record.getNodeId(), record.getSeqNo().get(record.getNodeId()));
            allSequences.put(record.getNodeId(), record.getSeqNo());
            allSequences.put(id, ImmutableMap.copyOf(sequences));
        }
    }

    public void heartbeat(Integer nodeId, ImmutableMap<Integer, Integer> sequence) {
        synchronized (lock) {
            sequences.put(nodeId, sequence.get(nodeId));
            allSequences.put(nodeId, sequence);
            allSequences.put(id, ImmutableMap.copyOf(sequences));
        }
    }

    public ImmutableList<Record<T>> records2() {
        final ImmutableList<Record<T>> recordSnapshot;
        final ImmutableMap<Integer, ImmutableMap<Integer, Integer>> sequencesSnapshot;
        synchronized (lock) {
            recordSnapshot = ImmutableList.copyOf(records);
            sequencesSnapshot = ImmutableMap.copyOf(allSequences);
        }
        System.out.println("all records: "+recordSnapshot);
        System.out.println("Sequences: "+sequencesSnapshot);
        final Map<Integer, Integer> commonRecord = new HashMap<>();
        for (Integer nodeId : nodeIds()) {
//            System.out.println("Doing node id "+nodeId);
            ImmutableMap<Integer, Integer> nodesConceptOfSequence = sequencesSnapshot.get(nodeId);
//            System.out.println("nodesConceptOfSequence: "+nodesConceptOfSequence);

            for (Integer nodeNodeId : nodeIds()) {
                Integer currentSequence = commonRecord.get(nodeNodeId);
//                System.out.println("currentSequence: "+currentSequence);
                Integer remoteSequence;
                if (nodesConceptOfSequence != null) {
                    remoteSequence = nodesConceptOfSequence.get(nodeNodeId);
                    if (remoteSequence == null) {
                        remoteSequence = -1;
                    }
                } else {
                    remoteSequence = -1;
                }
//                System.out.println("remoteSequence: "+remoteSequence);
                if (currentSequence == null || remoteSequence < currentSequence) {
//                    System.out.println("updating lowest common denom "+nodeNodeId+"="+remoteSequence);
                    commonRecord.put(nodeNodeId, remoteSequence);
                }
            }

        }
        System.out.println("Lowest common denominator: "+commonRecord);
        return from(ImmutableList.copyOf(recordSnapshot)).filter(new Predicate<Record<T>>() {
            @Override
            public boolean apply(final Record<T> record) {
                return from(nodeIds()).allMatch(new Predicate<Integer>() {
                    @Override
                    public boolean apply(Integer nodeId) {
                        return commonRecord.get(nodeId) > record.getSeqNo().get(nodeId);
                    }
                });
            }
        }).toList();
    }

    private Set<Integer> nodeIds() {
        HashSet<Integer> nodeIds = new HashSet<>(from(otherNodes).transform(new Function<Node<T>, Integer>() {
            @Override
            public Integer apply(Node<T> node) {
                return node.id;
            }
        }).toSet());
        nodeIds.add(id);
        return nodeIds;
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
