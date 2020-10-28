package grakn.common.poc.reasoning;

import java.util.Iterator;

/**
 * Pretend to compute an iterator of longs, which just adds up numbers and returns
 * them in intervals.
 *
 * Repeatedly calling query() will repeat the same calculation loop in a new iterator
 */
public class MockTransaction {
    private long computeLength;
    private int answerInterval;

    public MockTransaction(long computeLength, int answerInterval) {
        this.computeLength = computeLength;
        this.answerInterval = answerInterval;
    }

    public Iterator<Long> query() {
        return new Iterator<Long>() {
            long count = 0L;

            @Override
            public boolean hasNext() {
                return count < computeLength;
            }

            @Override
            public Long next() {
                while (count < computeLength) {
                    if (count % answerInterval == 0) {
                        count++;
                        return count - 1;
                    } else {
                        count++;
                    }
                }
                return null;
            }
        };
    }
}
