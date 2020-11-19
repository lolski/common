package grakn.common.poc.reasoning.framework;

import grakn.common.concurrent.actor.Actor;

import java.util.HashMap;
import java.util.Map;

import static grakn.common.collection.Collections.map;

public class Resolutions {
    public static final Resolutions EMPTY = new Resolutions(map());

    private Map<Actor<? extends Resolver<?>>, Response.Answer> answers;

    public Resolutions(Map<Actor<? extends Resolver<?>>, Response.Answer> answers) {
        this.answers = map(answers);
    }

    public Resolutions withAnswer(Actor<? extends Resolver<?>> producer, Response.Answer answer) {
        Map<Actor<? extends Resolver<?>>, Response.Answer> copiedDerivations = new HashMap<>(answers);
        copiedDerivations.put(producer, answer);
        return new Resolutions(copiedDerivations);
    }

    public void update(Map<Actor<? extends Resolver<?>>, Response.Answer> newDerivations) {
        assert answers.keySet().stream().noneMatch(key -> answers.containsKey(key)) : "Cannot overwrite any resolutions during an update";
        Map<Actor<? extends Resolver<?>>, Response.Answer> copiedDerivations = new HashMap<>(answers);
        copiedDerivations.putAll(newDerivations);
        this.answers = copiedDerivations;
    }

    public void replace(Map<Actor<? extends Resolver<?>>, Response.Answer> newDerivations) {
        this.answers = map(newDerivations);
    }

    public Map<Actor<? extends Resolver<?>>, Response.Answer> answers() {
        return this.answers;
    }

    @Override
    public String toString() {
        return "Derivations{" + "answers=" + answers + '}';
    }
}
