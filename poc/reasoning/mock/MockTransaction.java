package grakn.common.poc.reasoning.mock;

import java.util.Iterator;
import java.util.List;

import static grakn.common.collection.Collections.extend;

/**
 * Pretend to compute an iterator of longs, which just adds up numbers and returns
 * them in intervals.
 *
 * Repeatedly calling query() will repeat the same calculation loop in a new iterator
 */
public class MockTransaction {
    private final long computeLength;
    private final Long traversalPattern;
    private final int conceptMapInterval;

    public MockTransaction(long computeLength, Long traversalPattern, int conceptMapInterval) {
        this.computeLength = computeLength + traversalPattern;
        this.traversalPattern = traversalPattern;
        this.conceptMapInterval = conceptMapInterval;

    }

    public Iterator<List<Long>> query(List<Long> partialConceptMap) {
        return new Iterator<List<Long>>() {
            long count = traversalPattern;

            @Override
            public boolean hasNext() {
                return count < computeLength;
            }

            @Override
            public List<Long> next() {
                while (count < computeLength) {
                    if (count % conceptMapInterval == 0) {
                        List<Long> conceptMap = extend(partialConceptMap, count);
                        count++;
                        return conceptMap;
                    } else {
                        count++;
                    }
                }
                return null;
            }
        };
    }
}
