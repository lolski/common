package grakn.common.poc.reasoning.execution.framework;

import grakn.common.concurrent.actor.Actor;

import java.util.HashMap;
import java.util.Map;

import static grakn.common.collection.Collections.map;

public class ExecutionRecord {
    public static final ExecutionRecord EMPTY = new ExecutionRecord(map());

    private Map<Actor<? extends ExecutionActor<?>>, Answer> answers;

    public ExecutionRecord(Map<Actor<? extends ExecutionActor<?>>, Answer> answers) {
        this.answers = map(answers);
    }

    public ExecutionRecord withAnswer(Actor<? extends ExecutionActor<?>> producer, Answer answer) {
        Map<Actor<? extends ExecutionActor<?>>, Answer> copiedResolution = new HashMap<>(answers);
        copiedResolution.put(producer, answer);
        return new ExecutionRecord(copiedResolution);
    }

    public void update(Map<Actor<? extends ExecutionActor<?>>, Answer> newResolutions) {
        assert answers.keySet().stream().noneMatch(key -> answers.containsKey(key)) : "Cannot overwrite any derivations during an update";
        Map<Actor<? extends ExecutionActor<?>>, Answer> copiedResolutinos = new HashMap<>(answers);
        copiedResolutinos.putAll(newResolutions);
        this.answers = copiedResolutinos;
    }

    public void replace(Map<Actor<? extends ExecutionActor<?>>, Answer> newResolutions) {
        this.answers = map(newResolutions);
    }

    public Map<Actor<? extends ExecutionActor<?>>, Answer> answers() {
        return this.answers;
    }

    @Override
    public String toString() {
        return "ExecutionRecord{" + "answers=" + answers + '}';
    }
}
