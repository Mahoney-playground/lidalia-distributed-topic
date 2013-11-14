import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.google.common.base.Function;

import uk.org.lidalia.distributedtopic.Message;
import uk.org.lidalia.distributedtopic.TopicNode;

import static com.google.common.collect.FluentIterable.from;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class SingleNodeTest {

    @Test
    public void recordsReturnedInInsertionOrder() {
        TopicNode node = new TopicNode(1);
        for (int i = 1; i <= 100; i++) {
            node.store(i);
        }
        assertThat(from(node.allMessages()).transform(toPayload()).toList(), is(list(1, 100)));
    }

    private Function<Message, Integer> toPayload() {
        return new Function<Message, Integer>() {
            @Override
            public Integer apply(Message message) {
                return (Integer) message.get();
            }
        };
    }

    public static List<Integer> list(int i, int i1) {
        List<Integer> result = new ArrayList<>();
        for (; i <= i1; i++) {
            result.add(i);
        }
        return result;
    }
}
