package grakn.common.poc.reasoning.framework;

import grakn.common.concurrent.actor.Actor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static grakn.common.collection.Collections.map;

public class ResolutionTree extends Actor.State<ResolutionTree> {
    private static final Logger LOG = LoggerFactory.getLogger(ResolutionTree.class);

    Map<AnswerIndex, Resolver.Response.Answer> answers;

    public ResolutionTree(final Actor<ResolutionTree> self) {
        super(self);
        answers = new HashMap<>();
    }

    @Override
    protected void exception(final Exception e) {
        LOG.error("Actor exception", e);
    }

    public void grow(Resolver.Response.Answer answer) {
        merge(answer);
    }

    /**
     * Recursively merge resolution tree nodes into the existing resolution nodes that are recorded in the
     * answer index. Always keep the pre-existing resolution node, and merge the new ones into the existing node.
     */
    private Resolver.Response.Answer merge(Resolver.Response.Answer newAnswer) {

        Resolver.Response.Answer.Resolution newResolution = newAnswer.resolutions();
        Map<Actor<? extends Resolver<?>>, Resolver.Response.Answer> subAnswers = newResolution.answers();

        Map<Actor<? extends Resolver<?>>, Resolver.Response.Answer> mergedSubAnswers = new HashMap<>();
        for (Actor<? extends Resolver<?>> key : subAnswers.keySet()) {
            Resolver.Response.Answer subAnswer = subAnswers.get(key);
            Resolver.Response.Answer mergedSubAnswer = merge(subAnswer);
            mergedSubAnswers.put(key, mergedSubAnswer);
        }
        newResolution.replace(mergedSubAnswers);

        AnswerIndex newAnswerIndex = new AnswerIndex(newAnswer.sourceRequest().receiver(), newAnswer.conceptMap());
        if (answers.containsKey(newAnswerIndex)) {
            Resolver.Response.Answer existingAnswer = answers.get(newAnswerIndex);
            Resolver.Response.Answer.Resolution existingResolution = existingAnswer.resolutions();
            existingResolution.update(newResolution.answers());
            return existingAnswer;
        } else {
            answers.put(newAnswerIndex, newAnswer);
            return newAnswer;
        }
    }

    static class AnswerIndex {
        private final Actor<? extends Resolver<?>> producer;
        private final List<Long> conceptMap;

        public AnswerIndex(final Actor<? extends Resolver<?>> producer, final List<Long> conceptMap) {
            this.producer = producer;
            this.conceptMap = conceptMap;
        }

        @Override
        public String toString() {
            return "AnswerIndex{" +
                    "producer=" + producer.state.name() +
                    ", conceptMap=" + conceptMap +
                    '}';
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final AnswerIndex that = (AnswerIndex) o;
            return Objects.equals(producer, that.producer) &&
                    Objects.equals(conceptMap, that.conceptMap);
        }

        @Override
        public int hashCode() {
            return Objects.hash(producer, conceptMap);
        }
    }

}
