import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import uk.org.lidalia.distributedtopic.Node;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class SingleNodeTest {

    @Test
    public void recordsReturnedInInsertionOrder() {
        Node<Integer> node = new Node<>(1);
        for (int i = 1; i <= 100; i++) {
            node.store(i);
        }
        assertThat(node.records(), is(list(1, 100)));
    }

    public static List<Integer> list(int i, int i1) {
        List<Integer> result = new ArrayList<>();
        for (; i <= i1; i++) {
            result.add(i);
        }
        return result;
    }
}
