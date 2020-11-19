package grakn.common.poc.reasoning;

import grakn.common.concurrent.actor.Actor;
import grakn.common.poc.reasoning.framework.Derivations;
import grakn.common.poc.reasoning.framework.ExecutionActor;
import grakn.common.poc.reasoning.framework.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class ExecutionRecorder extends Actor.State<ExecutionRecorder> {
    private static final Logger LOG = LoggerFactory.getLogger(ExecutionRecorder.class);

    Map<AnswerIndex, Response.Answer> answerIndex;

    public ExecutionRecorder(final Actor<ExecutionRecorder> self) {
        super(self);
        answerIndex = new HashMap<>();
    }

    @Override
    protected void exception(final Exception e) {
        LOG.error("Actor exception", e);
    }

    public void recordTree(Response.Answer answer) {
        merge(answer);
    }

    /**
     * Recursively merge derivation tree nodes into the existing derivation nodes that are recorded in the
     * answer index. Always keep the pre-existing derivation node, and merge the new ones into the existing node.
     */
    private Response.Answer merge(Response.Answer newAnswer) {

        Derivations newDerivations = newAnswer.derivations();
        Map<Actor<? extends ExecutionActor<?>>, Response.Answer> subAnswers = newDerivations.answers();

        Map<Actor<? extends ExecutionActor<?>>, Response.Answer> mergedSubAnswers = new HashMap<>();
        for (Actor<? extends ExecutionActor<?>> key : subAnswers.keySet()) {
            Response.Answer subAnswer = subAnswers.get(key);
            Response.Answer mergedSubAnswer = merge(subAnswer);
            mergedSubAnswers.put(key, mergedSubAnswer);
        }
        newDerivations.replace(mergedSubAnswers);

        AnswerIndex newAnswerIndex = new AnswerIndex(newAnswer.sourceRequest().receiver(), newAnswer.partialAnswer());
        if (answerIndex.containsKey(newAnswerIndex)) {
            Response.Answer existingAnswer = answerIndex.get(newAnswerIndex);
            Derivations existingDerivations = existingAnswer.derivations();
            existingDerivations.update(newDerivations.answers());
            return existingAnswer;
        } else {
            answerIndex.put(newAnswerIndex, newAnswer);
            return newAnswer;
        }
    }

    static class AnswerIndex {
        private final Actor<? extends ExecutionActor<?>> producer;
        private List<Long> conceptMap;

        public AnswerIndex(final Actor<? extends ExecutionActor<?>> producer, final List<Long> conceptMap) {
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
