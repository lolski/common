package grakn.common.poc.reasoning.execution.framework;

import grakn.common.concurrent.actor.Actor;

import java.util.HashMap;
import java.util.Map;

import static grakn.common.collection.Collections.map;

public class Derivations {
    public static final Derivations EMPTY = new Derivations(map());

    private Map<Actor<? extends ExecutionActor<?>>, Response.Answer> answers;

    public Derivations(Map<Actor<? extends ExecutionActor<?>>, Response.Answer> answers) {
        this.answers = map(answers);
    }

    public Derivations withAnswer(Actor<? extends ExecutionActor<?>> producer, Response.Answer answer) {
        Map<Actor<? extends ExecutionActor<?>>, Response.Answer> copiedResolution = new HashMap<>(answers);
        copiedResolution.put(producer, answer);
        return new Derivations(copiedResolution);
    }

    public void update(Map<Actor<? extends ExecutionActor<?>>, Response.Answer> newResolutions) {
        assert answers.keySet().stream().noneMatch(key -> answers.containsKey(key)) : "Cannot overwrite any derivations during an update";
        Map<Actor<? extends ExecutionActor<?>>, Response.Answer> copiedResolutinos = new HashMap<>(answers);
        copiedResolutinos.putAll(newResolutions);
        this.answers = copiedResolutinos;
    }

    public void replace(Map<Actor<? extends ExecutionActor<?>>, Response.Answer> newResolutions) {
        this.answers = map(newResolutions);
    }

    public Map<Actor<? extends ExecutionActor<?>>, Response.Answer> answers() {
        return this.answers;
    }

    @Override
    public String toString() {
        return "Resolutions{" + "answers=" + answers + '}';
    }
}
