package grakn.common.poc.reasoning.framework;

import grakn.common.concurrent.actor.Actor;

import java.util.HashMap;
import java.util.Map;

import static grakn.common.collection.Collections.map;

public class Derivations {
    public static final Derivations EMPTY = new Derivations(map());

    private Map<Actor<? extends Execution<?>>, Response.Answer> answers;

    public Derivations(Map<Actor<? extends Execution<?>>, Response.Answer> answers) {
        this.answers = map(answers);
    }

    public Derivations withAnswer(Actor<? extends Execution<?>> producer, Response.Answer answer) {
        Map<Actor<? extends Execution<?>>, Response.Answer> copiedDerivations = new HashMap<>(answers);
        copiedDerivations.put(producer, answer);
        return new Derivations(copiedDerivations);
    }

    public void update(Map<Actor<? extends Execution<?>>, Response.Answer> newDerivations) {
        assert answers.keySet().stream().noneMatch(key -> answers.containsKey(key)) : "Cannot overwrite any derivations during an update";
        Map<Actor<? extends Execution<?>>, Response.Answer> copiedDerivations = new HashMap<>(answers);
        copiedDerivations.putAll(newDerivations);
        this.answers = copiedDerivations;
    }

    public void replace(Map<Actor<? extends Execution<?>>, Response.Answer> newDerivations) {
        this.answers = map(newDerivations);
    }

    public Map<Actor<? extends Execution<?>>, Response.Answer> answers() {
        return this.answers;
    }

    @Override
    public String toString() {
        return "Derivations{" + "answers=" + answers + '}';
    }
}
