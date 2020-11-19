package grakn.common.poc.reasoning;

import grakn.common.concurrent.actor.Actor;
import grakn.common.poc.reasoning.framework.resolver.Response;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

public class Root extends Conjunction<Root> {
    public Root(Actor<Root> self, List<Long> conjunction,
                Long traversalSize, Long traversalOffset, LinkedBlockingQueue<Response> responses) {
        super(self, Root.class.getSimpleName() + "(pattern:" + conjunction + ")", conjunction, traversalSize, traversalOffset, responses);
    }
}
