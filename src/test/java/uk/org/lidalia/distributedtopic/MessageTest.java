package uk.org.lidalia.distributedtopic;

import org.joda.time.DateTimeUtils;
import org.junit.Test;

import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class MessageTest {

    @Test
    public void orderingSameNodeId() {
        Message message1 = new Message(1, new SingleNodeVectorClock(new NodeId(1)));
        Message message2 = new Message(2, message1.getVectorClock().next());

        ImmutableSortedSet<Message> ordered = ImmutableSortedSet.of(message1, message2);

        assertThat(ordered.first(), is(message1));
        assertThat(ordered.last(), is(message2));
    }
}
