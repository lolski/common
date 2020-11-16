package grakn.common.poc.reasoning;

import grakn.common.concurrent.actor.Actor;

import java.util.HashMap;
import java.util.List;
import java.util.function.Function;

public class Registry {
    private HashMap<Long, Actor<Concludable>> atomicActors;
    private HashMap<List<Long>, Actor<Rule>> ruleActors;

    public Registry() {
        this.atomicActors = new HashMap<>();
        this.ruleActors = new HashMap<>();
    }

    public synchronized Actor<Concludable> registerAtomic(Long pattern, Function<Long, Actor<Concludable>> actorConstructor) {
        return atomicActors.computeIfAbsent(pattern, actorConstructor);
    }

    public synchronized Actor<Rule> registerRule(List<Long> pattern, Function<List<Long>, Actor<Rule>> actorConstructor) {
        return ruleActors.computeIfAbsent(pattern, actorConstructor);
    }
}
