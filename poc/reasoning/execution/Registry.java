package grakn.common.poc.reasoning.execution;

import grakn.common.concurrent.actor.Actor;
import grakn.common.concurrent.actor.eventloop.EventLoopGroup;
import grakn.common.poc.reasoning.execution.actor.Concludable;
import grakn.common.poc.reasoning.execution.actor.Rule;

import java.util.HashMap;
import java.util.List;
import java.util.function.Function;

public class Registry {
    private final HashMap<Long, Actor<Concludable>> concludables;
    private final HashMap<List<Long>, Actor<Rule>> rules;
    private final Actor<ExecutionRecorder> executionRecorder;

    public Registry(EventLoopGroup elg) {
        concludables = new HashMap<>();
        rules = new HashMap<>();
        executionRecorder = Actor.create(elg, ExecutionRecorder::new);
    }

    public Actor<Concludable> registerConcludable(Long pattern, Function<Long, Actor<Concludable>> actorConstructor) {
        return concludables.computeIfAbsent(pattern, actorConstructor);
    }

    public Actor<Rule> registerRule(List<Long> pattern, Function<List<Long>, Actor<Rule>> actorConstructor) {
        return rules.computeIfAbsent(pattern, actorConstructor);
    }

    public Actor<ExecutionRecorder> executionRecorder() {
        return executionRecorder;
    }
}
