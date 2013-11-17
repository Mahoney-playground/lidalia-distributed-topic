package uk.org.lidalia.distributedtopic;

import com.google.common.collect.ImmutableSortedMap;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class VectorClockTest {

    @Test
    public void getLowestCommonClock() {
        VectorClock vectorClock1 = new VectorClock(new NodeId(1)).add(new NodeId(2));
        SingleNodeVectorClock localVectorClockOnNode2 = new SingleNodeVectorClock(new NodeId(2)).add(new NodeId(1)).next();
        VectorClock vectorClock2 = vectorClock1.update(localVectorClockOnNode2);

        assertThat(vectorClock2.getLowestCommonClock().getState(), is(ImmutableSortedMap.<NodeId, Integer>naturalOrder()
                .put(new NodeId(1), 0)
                .put(new NodeId(2), 2)
                .build()));
    }

    @Test
    public void update() {
        VectorClock vectorClock1 = new VectorClock(new NodeId(1)).add(new NodeId(2));
        SingleNodeVectorClock localVectorClockOnNode2 = new SingleNodeVectorClock(new NodeId(2)).add(new NodeId(1)).next().next();
        VectorClock vectorClock2 = vectorClock1.update(localVectorClockOnNode2);

        ImmutableSortedMap<NodeId, SingleNodeVectorClock> expected =
                ImmutableSortedMap.<NodeId, SingleNodeVectorClock>naturalOrder()
                        .put(new NodeId(1), new SingleNodeVectorClock(new NodeId(1)).add(new NodeId(2)).update(new NodeId(2), 3))
                        .put(new NodeId(2), new SingleNodeVectorClock(new NodeId(2)).add(new NodeId(1)).next().next())
                        .build();
        assertThat(vectorClock2.getState(), is(expected));
    }
}
