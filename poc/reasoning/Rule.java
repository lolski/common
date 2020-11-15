package grakn.common.poc.reasoning;

import grakn.common.concurrent.actor.Actor;

import java.util.List;

// TODO unify and materialise in receiveAnswer
public class Rule extends AbstractConjunction<Rule> {
    public Rule(final Actor<Rule> self, final List<Long> when,
                final Long traversalSize) {
        super(self, Rule.class.getSimpleName() + "(pattern:" + when + ")", when, traversalSize, null);
    }
}
