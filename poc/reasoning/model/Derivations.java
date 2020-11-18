package grakn.common.poc.reasoning.model;

import grakn.common.concurrent.actor.Actor;

import java.util.HashMap;
import java.util.Map;

import static grakn.common.collection.Collections.map;

public class Derivations {
    public static final Derivations EMPTY = new Derivations(map());

    private final Map<Actor<? extends ExecutionActor<?>>, Response.Answer> answers;

    public Derivations(Map<Actor<? extends ExecutionActor<?>>, Response.Answer> answers) {
        this.answers = map(answers);
    }

    public Derivations withAnswer(Actor<? extends ExecutionActor<?>> producer, Response.Answer answer) {
        Map<Actor<? extends ExecutionActor<?>>, Response.Answer> copiedInferences = new HashMap<>(answers);
        copiedInferences.put(producer, answer);
        return new Derivations(copiedInferences);
    }

    public Map<Actor<? extends ExecutionActor<?>>, Response.Answer> answers() {
        return this.answers;
    }

    @Override
    public String toString() {
        return "Derivations{" + "answers=" + answers + '}';
    }
}
