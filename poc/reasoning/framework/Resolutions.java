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
        Map<Actor<? extends Resolver<?>>, Response.Answer> copiedResolution = new HashMap<>(answers);
        copiedResolution.put(producer, answer);
        return new Resolutions(copiedResolution);
    }

    public void update(Map<Actor<? extends Resolver<?>>, Response.Answer> newResolutions) {
        assert answers.keySet().stream().noneMatch(key -> answers.containsKey(key)) : "Cannot overwrite any resolutions during an update";
        Map<Actor<? extends Resolver<?>>, Response.Answer> copiedResolutinos = new HashMap<>(answers);
        copiedResolutinos.putAll(newResolutions);
        this.answers = copiedResolutinos;
    }

    public void replace(Map<Actor<? extends Resolver<?>>, Response.Answer> newResolutions) {
        this.answers = map(newResolutions);
    }

    public Map<Actor<? extends Resolver<?>>, Response.Answer> answers() {
        return this.answers;
    }

    @Override
    public String toString() {
        return "Resolutions{" + "answers=" + answers + '}';
    }
}
