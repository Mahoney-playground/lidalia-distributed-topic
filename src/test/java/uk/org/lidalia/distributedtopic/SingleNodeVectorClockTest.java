package uk.org.lidalia.distributedtopic;

import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;

import org.joda.time.DateTimeUtils;
import org.junit.Rule;
import org.junit.Test;

import uk.org.lidalia.test.StaticTimeRule;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class SingleNodeVectorClockTest {

    @Rule public StaticTimeRule staticTimeRule = StaticTimeRule.alwaysNow();

    @Test
    public void orderingSameNodeId() {
        SingleNodeVectorClock clock1 = new SingleNodeVectorClock(new NodeId(1));
        SingleNodeVectorClock clock2 = clock1.next();

        ImmutableSortedSet<SingleNodeVectorClock> ordered = ImmutableSortedSet.of(clock1, clock2);

        assertThat(ordered.first(), is(clock1));
        assertThat(ordered.last(), is(clock2));
    }

    @Test
    public void orderingOneBeforeOther() {
        SingleNodeVectorClock clock1 = new SingleNodeVectorClock(new NodeId(1), ImmutableSortedMap.<NodeId, Integer>naturalOrder()
                .put(new NodeId(1), 1)
                .put(new NodeId(2), 1)
                .build());
        SingleNodeVectorClock clock2 = new SingleNodeVectorClock(new NodeId(2), ImmutableSortedMap.<NodeId, Integer>naturalOrder()
                .put(new NodeId(1), 1)
                .put(new NodeId(2), 2)
                .build());

        ImmutableSortedSet<SingleNodeVectorClock> ordered = ImmutableSortedSet.of(clock1, clock2);

        assertThat(ordered.first(), is(clock1));
        assertThat(ordered.last(), is(clock2));
    }

    @Test
    public void orderingIndeterminateSoOrderByTime() {
        DateTimeUtils.setCurrentMillisFixed(1L);
        SingleNodeVectorClock clock1 = new SingleNodeVectorClock(new NodeId(1), ImmutableSortedMap.<NodeId, Integer>naturalOrder()
                .put(new NodeId(1), 1)
                .put(new NodeId(2), 2)
                .build());

        DateTimeUtils.setCurrentMillisFixed(2L);
        SingleNodeVectorClock clock2 = new SingleNodeVectorClock(new NodeId(2), ImmutableSortedMap.<NodeId, Integer>naturalOrder()
                .put(new NodeId(1), 2)
                .put(new NodeId(2), 1)
                .build());

        ImmutableSortedSet<SingleNodeVectorClock> ordered = ImmutableSortedSet.of(clock1, clock2);

        assertThat(ordered.first(), is(clock1));
        assertThat(ordered.last(), is(clock2));
    }

    @Test
    public void orderingIndeterminateSameTimeSoOrderBySeqDiff() {
        SingleNodeVectorClock clock1 = new SingleNodeVectorClock(new NodeId(1), ImmutableSortedMap.<NodeId, Integer>naturalOrder()
                .put(new NodeId(1), 1)
                .put(new NodeId(2), 2)
                .build());

        SingleNodeVectorClock clock2 = new SingleNodeVectorClock(new NodeId(2), ImmutableSortedMap.<NodeId, Integer>naturalOrder()
                .put(new NodeId(1), 3)
                .put(new NodeId(2), 1)
                .build());

        ImmutableSortedSet<SingleNodeVectorClock> ordered = ImmutableSortedSet.of(clock1, clock2);

        assertThat(ordered.first(), is(clock1));
        assertThat(ordered.last(), is(clock2));
    }

    @Test
    public void orderingIndeterminateSameTimeSeqDiffSameSoOrderByNodeSeqDiff() {
        SingleNodeVectorClock clock1 = new SingleNodeVectorClock(new NodeId(1), ImmutableSortedMap.<NodeId, Integer>naturalOrder()
                .put(new NodeId(1), 1)
                .put(new NodeId(2), 3)
                .build());

        SingleNodeVectorClock clock2 = new SingleNodeVectorClock(new NodeId(2), ImmutableSortedMap.<NodeId, Integer>naturalOrder()
                .put(new NodeId(1), 3)
                .put(new NodeId(2), 1)
                .build());

        ImmutableSortedSet<SingleNodeVectorClock> ordered = ImmutableSortedSet.of(clock1, clock2);

        assertThat(ordered.first(), is(clock1));
        assertThat(ordered.last(), is(clock2));
    }
}
