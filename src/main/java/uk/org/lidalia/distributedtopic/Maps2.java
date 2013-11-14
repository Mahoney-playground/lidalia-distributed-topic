package uk.org.lidalia.distributedtopic;

import com.google.common.base.Function;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Multimap;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.collect.Iterables.transform;

public final class Maps2 {

    private Maps2() {
        throw new UnsupportedOperationException("Not instantiable.");
    }

    public static <K, V, I> ImmutableSortedMap<K, V> uniqueIndex(Iterable<I> input, Function<? super I, Map.Entry<K, V>> transformer) {
        Iterable<Map.Entry<K, V>> entries = transform(input, transformer);
        Map<K, V> buffer = new HashMap<>();
        for (Map.Entry<K, V> entry : entries) {
            V existing = buffer.put(entry.getKey(), entry.getValue());
            if (existing != null) {
                throw new IllegalArgumentException("Duplicate key " + entry.getKey() + " in " + input);
            }
        }
        return ImmutableSortedMap.copyOf(buffer);
    }

    public static <K, V, I> ImmutableMultimap<K, V> index(Iterable<I> input, Function<? super I, Map.Entry<K, V>> transformer) {
        Iterable<Map.Entry<K, V>> entries = transform(input, transformer);
        Multimap<K, V> buffer = ArrayListMultimap.create();
        for (Map.Entry<K, V> entry : entries) {
            buffer.put(entry.getKey(), entry.getValue());
        }
        return ImmutableMultimap.copyOf(buffer);
    }

    public static <K,V> ImmutableSortedMap<K,V> put(ImmutableMap<K,V> initialState, K key, V value) {
        Map<K, V> map = new HashMap<>(initialState);
        map.put(key, value);
        return ImmutableSortedMap.copyOf(map);
    }
}
