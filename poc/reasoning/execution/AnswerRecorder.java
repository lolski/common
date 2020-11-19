package grakn.common.poc.reasoning.execution;

import grakn.common.concurrent.actor.Actor;
import grakn.common.poc.reasoning.execution.framework.Answer;
import grakn.common.poc.reasoning.execution.framework.ExecutionRecord;
import grakn.common.poc.reasoning.execution.framework.ExecutionActor;
import grakn.common.poc.reasoning.execution.framework.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static grakn.common.collection.Collections.map;

public class AnswerRecorder extends Actor.State<AnswerRecorder> {
    private static final Logger LOG = LoggerFactory.getLogger(AnswerRecorder.class);

    Map<AnswerIndex, Answer> answers;

    public AnswerRecorder(final Actor<AnswerRecorder> self) {
        super(self);
        answers = new HashMap<>();
    }

    @Override
    protected void exception(final Exception e) {
        LOG.error("Actor exception", e);
    }

    public void record(Answer answer) {
        merge(answer);
    }

    /**
     * Recursively merge derivation tree nodes into the existing derivation nodes that are recorded in the
     * answer index. Always keep the pre-existing derivation node, and merge the new ones into the existing node.
     */
    private Answer merge(Answer newAnswer) {

        ExecutionRecord newExecutionRecord = newAnswer.executionRecord();
        Map<Actor<? extends ExecutionActor<?>>, Answer> subAnswers = newExecutionRecord.answers();

        Map<Actor<? extends ExecutionActor<?>>, Answer> mergedSubAnswers = new HashMap<>();
        for (Actor<? extends ExecutionActor<?>> key : subAnswers.keySet()) {
            Answer subAnswer = subAnswers.get(key);
            Answer mergedSubAnswer = merge(subAnswer);
            mergedSubAnswers.put(key, mergedSubAnswer);
        }
        newExecutionRecord.replace(mergedSubAnswers);

        AnswerIndex newAnswerIndex = new AnswerIndex(newAnswer.producer(), newAnswer.conceptMap());
        if (answers.containsKey(newAnswerIndex)) {
            Answer existingAnswer = answers.get(newAnswerIndex);
            ExecutionRecord existingExecutionRecord = existingAnswer.executionRecord();
            existingExecutionRecord.update(newExecutionRecord.answers());
            return existingAnswer;
        } else {
            answers.put(newAnswerIndex, newAnswer);
            return newAnswer;
        }
    }

    static class AnswerIndex {
        private final Actor<? extends ExecutionActor<?>> producer;
        private final List<Long> conceptMap;

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
