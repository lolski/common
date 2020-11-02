package grakn.common.poc.reasoning;

import grakn.common.concurrent.actor.Actor;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;

public class ActorRegistry {
    private ConcurrentHashMap<Long, Actor<AtomicActor>> atomicActors;
    private ConcurrentHashMap<List<Long>, Actor<RuleActor>> ruleActors;


    public ActorRegistry() {
        this.atomicActors = new ConcurrentHashMap<>();
        this.ruleActors = new ConcurrentHashMap<>();
    }

    public Actor<AtomicActor> registerAtomic(Long pattern, Function<Long, Actor<AtomicActor>> actorConstructor) {
        return atomicActors.computeIfAbsent(pattern, actorConstructor);
    }

    public Actor<RuleActor> registerRule(List<Long> pattern, Function<List<Long>, Actor<RuleActor>> actorConstructor) {
        return ruleActors.computeIfAbsent(pattern, actorConstructor);
    }
}
