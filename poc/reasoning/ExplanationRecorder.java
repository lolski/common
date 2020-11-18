package grakn.common.poc.reasoning;

import grakn.common.concurrent.actor.Actor;
import grakn.common.poc.reasoning.model.ExecutionActor;
import grakn.common.poc.reasoning.model.Explanation;
import grakn.common.poc.reasoning.model.Request;
import grakn.common.poc.reasoning.model.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class ExplanationRecorder extends Actor.State<ExplanationRecorder> {
    private static final Logger LOG = LoggerFactory.getLogger(ExplanationRecorder.class);

    Map<List<Long>, Response.Answer> rootAnswers;
    Map<AnswerIndex, Response.Answer> answerIndex;

    protected ExplanationRecorder(final Actor<ExplanationRecorder> self) {
        super(self);
    }

    public void recordDuplicateAnswer(final Actor<Concludable> deduplicator, final List<Long> conceptMap, final Response.Answer answer) {
        AnswerIndex index = new AnswerIndex(deduplicator, conceptMap);

        if (!answerIndex.containsKey(index)) {
            answerIndex.put(index, answer);
        } else {
            Explanation toMerge = answer.explanation();
            Map<String, Set<Explanation.Inference>> inferences = toMerge.inferences();





            String inferenceKey = inferences.keySet().iterator().next();
            answerIndex.get(index).explanation().withInference(inferenceKey, inferences.get(inferenceKey));
        }

        createIndices(answer.explanation());
    }

    private Response.Answer merge(Response.Answer existingAnswer, Response.Answer newAnswer) {
        Explanation existingExplanation = existingAnswer.explanation();
        Map<String, Set<Explanation.Inference>> existingInferences = existingExplanation.inferences();

        // recursive call first
        for (Set<Explanation.Inference> inferences : existingInferences.values()) {
            for (Explanation.Inference inference : inferences) {
                Response.Answer inferenceAnswer = inference.answer();
                AnswerIndex index = new AnswerIndex(inferenceAnswer.sourceRequest().receiver(), inferenceAnswer.partialAnswer());
            }
        }




        Map<String, Set<Explanation.Inference>> newInferences = newAnswer.explanation().inferences();
        existingExplanation.update(newInferences);

        return existingAnswer;
    }

    public void recordRootAnswer() {
        // TODO
    }

    public static void recordDuplicateAnswer(Request request, List<Long> partialAnswer, Explanation explanation) {
        AnswerIndex index = new AnswerIndex(request.path(), partialAnswer);
        if (!answerIndex.containsKey(index)) {
            throw new RuntimeException("Recording a duplicate answer failed as the answer was not indexed before");
        }
    }

    private void createIndices(Explanation explanation) {

    }

    @Override
    protected void exception(final Exception e) {
        LOG.error(e.toString());
    }


    static class AnswerIndex {
        private final Actor<Concludable> deduplicator;
        private List<Long> conceptMap;
        private final Actor<? extends ExecutionActor<?>> producer;

        public AnswerIndex(final Actor<Concludable> deduplicator, final List<Long> conceptMap) {
            this.deduplicator = deduplicator;
            this.conceptMap = this.conceptMap;
            this.producer = producer;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final AnswerIndex that = (AnswerIndex) o;
            return deduplicator.equals(that.deduplicator) &&
                    conceptMap.equals(that.conceptMap) &&
                    producer.equals(that.producer);
        }

        @Override
        public int hashCode() {
            return Objects.hash(deduplicator, conceptMap, producer);
        }
    }
}
