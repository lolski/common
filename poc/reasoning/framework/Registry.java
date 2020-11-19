package grakn.common.poc.reasoning.framework;

import grakn.common.concurrent.actor.Actor;
import grakn.common.concurrent.actor.eventloop.EventLoopGroup;
import grakn.common.poc.reasoning.Concludable;
import grakn.common.poc.reasoning.DerivationRecorder;
import grakn.common.poc.reasoning.Rule;

import java.util.HashMap;
import java.util.List;
import java.util.function.Function;

public class Registry {
    private final HashMap<Long, Actor<Concludable>> atomicActors;
    private final HashMap<List<Long>, Actor<Rule>> ruleActors;
    private final Actor<DerivationRecorder> explanationRecorder;

    public Registry(EventLoopGroup elg) {
        atomicActors = new HashMap<>();
        ruleActors = new HashMap<>();
        explanationRecorder = Actor.create(elg, DerivationRecorder::new);
    }

    public Actor<Concludable> registerAtomic(Long pattern, Function<Long, Actor<Concludable>> actorConstructor) {
        return atomicActors.computeIfAbsent(pattern, actorConstructor);
    }

    public Actor<Rule> registerRule(List<Long> pattern, Function<List<Long>, Actor<Rule>> actorConstructor) {
        return ruleActors.computeIfAbsent(pattern, actorConstructor);
    }

    public Actor<DerivationRecorder> explanationRecorder() {
        return explanationRecorder;
    }
}
