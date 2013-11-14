package uk.org.lidalia.distributedtopic;

import com.google.common.collect.ImmutableSet;
import org.junit.Test;

public class SingleNodeVectorClockTest {

    @Test
    public void clocksWithDifferentNodeIdsCannotBeEqual() {
        SingleNodeVectorClock clock1 = new SingleNodeVectorClock(new NodeId(1), ImmutableSet.of(new NodeId(2)));
        SingleNodeVectorClock clock2 = new SingleNodeVectorClock(new NodeId(2), ImmutableSet.of(new NodeId(1)));
    }
}
