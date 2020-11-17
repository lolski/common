package grakn.common.poc.reasoning;

import grakn.common.concurrent.actor.Actor;

import java.util.List;

// TODO unify and materialise in receiveAnswer
public class Rule extends AbstractConjunction<Rule> {
    public Rule(Actor<Rule> self, List<Long> when,
                Long traversalSize, Long traversalOffset) {
        super(self, Rule.class.getSimpleName() + "(pattern:" + when + ")", when, traversalSize, traversalOffset, null);
    }
}
