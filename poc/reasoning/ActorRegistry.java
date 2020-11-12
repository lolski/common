package grakn.common.poc.reasoning;

import grakn.common.concurrent.actor.Actor;

import java.util.HashMap;
import java.util.List;
import java.util.function.Function;

public class ActorRegistry {
    private HashMap<Long, Actor<Atomic>> atomicActors;
    private HashMap<List<Long>, Actor<Rule>> ruleActors;

    public ActorRegistry() {
        this.atomicActors = new HashMap<>();
        this.ruleActors = new HashMap<>();
    }

    public synchronized Actor<Atomic> registerAtomic(Long pattern, Function<Long, Actor<Atomic>> actorConstructor) {
        return atomicActors.computeIfAbsent(pattern, actorConstructor);
    }

    public synchronized Actor<Rule> registerRule(List<Long> pattern, Function<List<Long>, Actor<Rule>> actorConstructor) {
        return ruleActors.computeIfAbsent(pattern, actorConstructor);
    }
}