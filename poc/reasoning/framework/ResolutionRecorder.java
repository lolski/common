package grakn.common.poc.reasoning.framework;

import grakn.common.concurrent.actor.Actor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class ResolutionRecorder extends Actor.State<ResolutionRecorder> {
    private static final Logger LOG = LoggerFactory.getLogger(ResolutionRecorder.class);

    Map<AnswerIndex, Response.Answer> answerIndex;

    public ResolutionRecorder(final Actor<ResolutionRecorder> self) {
        super(self);
        answerIndex = new HashMap<>();
    }

    @Override
    protected void exception(final Exception e) {
        LOG.error("Actor exception", e);
    }

    public void record(Response.Answer answer) {
        merge(answer);
    }

    /**
     * Recursively merge resolution tree nodes into the existing resolution nodes that are recorded in the
     * answer index. Always keep the pre-existing resolution node, and merge the new ones into the existing node.
     */
    private Response.Answer merge(Response.Answer newAnswer) {

        Resolutions newResolutions = newAnswer.resolutions();
        Map<Actor<? extends Resolver<?>>, Response.Answer> subAnswers = newResolutions.answers();

        Map<Actor<? extends Resolver<?>>, Response.Answer> mergedSubAnswers = new HashMap<>();
        for (Actor<? extends Resolver<?>> key : subAnswers.keySet()) {
            Response.Answer subAnswer = subAnswers.get(key);
            Response.Answer mergedSubAnswer = merge(subAnswer);
            mergedSubAnswers.put(key, mergedSubAnswer);
        }
        newResolutions.replace(mergedSubAnswers);

        AnswerIndex newAnswerIndex = new AnswerIndex(newAnswer.sourceRequest().receiver(), newAnswer.conceptMap());
        if (answerIndex.containsKey(newAnswerIndex)) {
            Response.Answer existingAnswer = answerIndex.get(newAnswerIndex);
            Resolutions existingResolutions = existingAnswer.resolutions();
            existingResolutions.update(newResolutions.answers());
            return existingAnswer;
        } else {
            answerIndex.put(newAnswerIndex, newAnswer);
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
