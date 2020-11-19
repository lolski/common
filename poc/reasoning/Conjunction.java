package grakn.common.poc.reasoning;

import grakn.common.concurrent.actor.Actor;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

public class Conjunction extends AbstractConjunction<Conjunction> {
    public Conjunction(Actor<Conjunction> self, List<Long> conjunction,
                       Long traversalSize, Long traversalOffset, LinkedBlockingQueue<Response> responses) {
        super(self, Conjunction.class.getSimpleName() + "(pattern:" + conjunction + ")", conjunction, traversalSize, traversalOffset, responses);
    }
}
