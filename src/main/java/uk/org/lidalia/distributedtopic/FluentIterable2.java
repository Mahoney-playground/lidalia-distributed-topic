package uk.org.lidalia.distributedtopic;

import com.google.common.annotations.Beta;
import com.google.common.annotations.GwtIncompatible;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class FluentIterable2<E> implements Iterable<E> {

    public static <E> FluentIterable2<E> from(final Iterable<E> iterable) {
        return new FluentIterable2<>(FluentIterable.from(iterable));
    }

    private final FluentIterable<E> fluentIterable;

    private FluentIterable2(FluentIterable<E> fluentIterable) {
        this.fluentIterable = fluentIterable;
    }

    public Optional<E> first() {
        return fluentIterable.first();
    }

    @GwtIncompatible("Array.newArray(Class, int)")
    public E[] toArray(Class<E> type) {
        return fluentIterable.toArray(type);
    }

    public FluentIterable2<E> cycle() {
        return from(fluentIterable.cycle());
    }

    public <T> FluentIterable2<T> transform(Function<? super E, T> function) {
        return from(fluentIterable.transform(function));
    }

    public <K> ImmutableMap<K,E> uniqueIndex(Function<? super E, K> keyFunction) {
        return fluentIterable.uniqueIndex(keyFunction);
    }

    public <C extends Collection<? super E>> C copyInto(C collection) {
        return fluentIterable.copyInto(collection);
    }

    public E get(int position) {
        return fluentIterable.get(position);
    }

    public <V> ImmutableMap<E,V> toMap(Function<? super E, V> valueFunction) {
        return fluentIterable.toMap(valueFunction);
    }

    public int size() {
        return fluentIterable.size();
    }

    public ImmutableSet<E> toSet() {
        return fluentIterable.toSet();
    }

    public boolean contains(Object element) {
        return fluentIterable.contains(element);
    }

    public FluentIterable2<E> limit(int size) {
        return from(fluentIterable.limit(size));
    }

    public boolean isEmpty() {
        return fluentIterable.isEmpty();
    }

    public ImmutableList<E> toList() {
        return fluentIterable.toList();
    }

    @Beta
    public ImmutableList<E> toSortedList(Comparator<? super E> comparator) {
        return fluentIterable.toSortedList(comparator);
    }

    public FluentIterable2<E> filter(Predicate<? super E> predicate) {
        return from(fluentIterable.filter(predicate));
    }

    public boolean allMatch(Predicate<? super E> predicate) {
        return fluentIterable.allMatch(predicate);
    }

    public ImmutableSortedSet<E> toSortedSet(Comparator<? super E> comparator) {
        return fluentIterable.toSortedSet(comparator);
    }

    public <T> FluentIterable2<T> filter(Class<T> type) {
        return from(fluentIterable.filter(type));
    }

    public <T> FluentIterable2<T> transformAndConcat(Function<? super E, ? extends Iterable<? extends T>> function) {
        return from(fluentIterable.transformAndConcat(function));
    }

    public Optional<E> last() {
        return fluentIterable.last();
    }

    public <K> ImmutableListMultimap<K,E> index(Function<? super E, K> keyFunction) {
        return fluentIterable.index(keyFunction);
    }

    public FluentIterable2<E> skip(int numberToSkip) {
        return from(fluentIterable.skip(numberToSkip));
    }

    public Optional<E> firstMatch(Predicate<? super E> predicate) {
        return fluentIterable.firstMatch(predicate);
    }

    public boolean anyMatch(Predicate<? super E> predicate) {
        return fluentIterable.anyMatch(predicate);
    }

    @Override
    public String toString() {
        return fluentIterable.toString();
    }

    @Override
    public Iterator<E> iterator() {
        return fluentIterable.iterator();
    }

    public FluentIterable2<E> takeWhile(Predicate<? super E> predicate) {
        List<E> acc = new LinkedList<>();
        Iterator<E> iterator = iterator();
        while (iterator.hasNext()) {
            E candidate = iterator.next();
            if (predicate.apply(candidate)) {
                acc.add(candidate);
            } else {
                break;
            }
        }
        return from(acc);
    }
}
