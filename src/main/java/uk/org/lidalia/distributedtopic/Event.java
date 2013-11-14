package uk.org.lidalia.distributedtopic;

import static com.google.common.base.Preconditions.checkNotNull;

public class Event<T> implements Comparable<Event<T>> {

    private final T value;
    private final SingleNodeVectorClock vectorClock;

    public Event(T value, SingleNodeVectorClock vectorClock) {
        this.value = checkNotNull(value);
        this.vectorClock = checkNotNull(vectorClock);
    }

    public T get() {
        return value;
    }

    @Override
    public int compareTo(Event<T> o) {
        return o.vectorClock.compareTo(vectorClock);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Event event = (Event) o;

        return vectorClock.equals(event.vectorClock);

    }

    @Override
    public int hashCode() {
        return vectorClock.hashCode();
    }
}
