package grakn.common.poc.reasoning.framework.resolver;

import grakn.common.concurrent.actor.Actor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static grakn.common.collection.Collections.map;

public interface Response {
    Request sourceRequest();

    boolean isAnswer();
    boolean isExhausted();

    default Answer asAnswer() {
        throw new ClassCastException("Cannot cast " + this.getClass().getSimpleName() + " to " + Answer.class.getSimpleName());
    }

    default Exhausted asExhausted() {
        throw new ClassCastException("Cannot cast " + this.getClass().getSimpleName() + " to " + Exhausted.class.getSimpleName());
    }

    class Answer implements Response {
        private final Request sourceRequest;
        private final List<Long> conceptMap;
        private final List<Object> unifiers;

        private final String patternAnswered;
        private final Answer.Resolution resolution;

        public Answer(Request sourceRequest,
                      List<Long> conceptMap,
                      List<Object> unifiers,
                      String patternAnswered,
                      Answer.Resolution resolution) {
            this.sourceRequest = sourceRequest;
            this.conceptMap = conceptMap;
            this.unifiers = unifiers;
            this.patternAnswered = patternAnswered;
            this.resolution = resolution;
        }

        @Override
        public Request sourceRequest() {
            return sourceRequest;
        }

        public List<Long> conceptMap() {
            return conceptMap;
        }

        public List<Object> unifiers() {
            return unifiers;
        }

        public Answer.Resolution resolutions() {
            return resolution;
        }

        public boolean isInferred() {
            return !resolution.equals(Answer.Resolution.EMPTY);
        }

        @Override
        public boolean isAnswer() { return true; }

        @Override
        public boolean isExhausted() { return false; }

        @Override
        public Answer asAnswer() {
            return this;
        }

        @Override
        public String toString() {
            return "\nAnswer{" +
                    "\nsourceRequest=" + sourceRequest +
                    ",\n partialConceptMap=" + conceptMap +
                    ",\n unifiers=" + unifiers +
                    ",\n patternAnswered=" + patternAnswered +
                    ",\n resolutionsg=" + resolution +
                    '}';
        }

        public static class Resolution {
            public static final Answer.Resolution EMPTY = new Answer.Resolution(map());

            private Map<Actor<? extends Resolver<?>>, Answer> answers;

            public Resolution(Map<Actor<? extends Resolver<?>>, Answer> answers) {
                this.answers = map(answers);
            }

            public Answer.Resolution withAnswer(Actor<? extends Resolver<?>> producer, Answer answer) {
                Map<Actor<? extends Resolver<?>>, Answer> copiedResolution = new HashMap<>(answers);
                copiedResolution.put(producer, answer);
                return new Answer.Resolution(copiedResolution);
            }

            public void update(Map<Actor<? extends Resolver<?>>, Answer> newResolutions) {
                assert answers.keySet().stream().noneMatch(key -> answers.containsKey(key)) : "Cannot overwrite any resolutions during an update";
                Map<Actor<? extends Resolver<?>>, Answer> copiedResolutinos = new HashMap<>(answers);
                copiedResolutinos.putAll(newResolutions);
                this.answers = copiedResolutinos;
            }

            public void replace(Map<Actor<? extends Resolver<?>>, Answer> newResolutions) {
                this.answers = map(newResolutions);
            }

            public Map<Actor<? extends Resolver<?>>, Answer> answers() {
                return this.answers;
            }

            @Override
            public String toString() {
                return "Resolutions{" + "answers=" + answers + '}';
            }
        }
    }

    class Exhausted implements Response {
        private final Request sourceRequest;

        public Exhausted(Request sourceRequest) {
            this.sourceRequest = sourceRequest;
        }

        @Override
        public Request sourceRequest() {
            return sourceRequest;
        }

        @Override
        public boolean isAnswer() { return false; }

        @Override
        public boolean isExhausted() { return true; }

        @Override
        public Exhausted asExhausted() {
            return this;
        }


        @Override
        public String toString() {
            return "Exhausted{" +
                    "sourceRequest=" + sourceRequest +
                    '}';
        }
    }
}
