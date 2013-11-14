package uk.org.lidalia.distributedtopic;

import static com.google.common.base.Preconditions.checkNotNull;

public class Message implements Comparable<Message> {

    private final Object value;
    private final SingleNodeVectorClock vectorClock;

    public Message(Object value, SingleNodeVectorClock vectorClock) {
        this.value = checkNotNull(value);
        this.vectorClock = checkNotNull(vectorClock);
    }

    public Object get() {
        return value;
    }

    public SingleNodeVectorClock getVectorClock() {
        return vectorClock;
    }

    @Override
    public int compareTo(Message o) {
        return vectorClock.compareTo(o.vectorClock);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Message message = (Message) o;

        return vectorClock.equals(message.vectorClock);

    }

    @Override
    public int hashCode() {
        return vectorClock.hashCode();
    }

    @Override
    public String toString() {
        return "{"+value +", "+vectorClock+'}';
    }
}
