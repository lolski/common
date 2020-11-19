package grakn.common.poc.reasoning.execution.actor;

import grakn.common.concurrent.actor.Actor;
import grakn.common.poc.reasoning.execution.framework.Response;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

public class Root extends Conjunction<Root> {
    public Root(Actor<Root> self, List<Long> conjunction,
                Long traversalSize, Long traversalOffset, LinkedBlockingQueue<Response> responses) {
        super(self, Root.class.getSimpleName() + "(pattern:" + conjunction + ")", conjunction, traversalSize, traversalOffset, responses);
    }
}
