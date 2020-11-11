package grakn.common.poc.reasoning;

import grakn.common.concurrent.actor.Actor;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class ActorRegistry {
    private HashMap<Long, Actor<AtomicActor>> atomicActors;
    private HashMap<List<Long>, Actor<RuleActor>> ruleActors;

    public ActorRegistry() {
        this.atomicActors = new HashMap<>();
        this.ruleActors = new HashMap<>();
    }

    public synchronized Actor<AtomicActor> registerAtomic(Long pattern, Function<Long, Actor<AtomicActor>> actorConstructor) {
        return atomicActors.computeIfAbsent(pattern, actorConstructor);
    }

    public synchronized Actor<RuleActor> registerRule(List<Long> pattern, Function<List<Long>, Actor<RuleActor>> actorConstructor) {
        return ruleActors.computeIfAbsent(pattern, actorConstructor);
    }
}
