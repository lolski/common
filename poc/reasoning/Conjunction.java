package grakn.common.poc.reasoning;

import grakn.common.concurrent.actor.Actor;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

public class Conjunction extends AbstractConjunction<Conjunction> {
    public Conjunction(final Actor<Conjunction> self, final List<Long> conjunction,
                final Long traversalSize, final LinkedBlockingQueue<List<Long>> responses) {
        super(self, Conjunction.class.getSimpleName() + "(pattern:" + conjunction + ")", conjunction, traversalSize, responses);
    }
}
