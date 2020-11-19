package grakn.common.poc.reasoning;

import grakn.common.concurrent.actor.Actor;
import grakn.common.poc.reasoning.framework.ExecutionActor;
import grakn.common.poc.reasoning.framework.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class DerivationRecorder extends Actor.State<DerivationRecorder> {
    private static final Logger LOG = LoggerFactory.getLogger(DerivationRecorder.class);
//
//    Map<List<Long>, Response.Answer> rootAnswers;
//    Map<AnswerIndex, Response.Answer> answerIndex;
//
public DerivationRecorder(final Actor<DerivationRecorder> self) {
        super(self);
    }
//
//    public void recordDuplicateAnswer(final Actor<Concludable> deduplicator, final List<Long> conceptMap, final Response.Answer answer) {
//        AnswerIndex index = new AnswerIndex(deduplicator, conceptMap);
//
//        if (!answerIndex.containsKey(index)) {
//            answerIndex.put(index, answer);
//        } else {
//            Derivations toMerge = answer.explanation();
//            Map<String, Set<Derivations.Inference>> inferences = toMerge.answers();
//
//
//
//
//
//            String inferenceKey = inferences.keySet().iterator().next();
//            answerIndex.get(index).explanation().withAnswer(inferenceKey, inferences.get(inferenceKey));
//        }
//
//        createIndices(answer.explanation());
//    }
//
////    private Response.Answer merge(Response.Answer existingAnswer, Response.Answer newAnswer) {
//////        Explanation existingExplanation = existingAnswer.explanation();
//////        Map<String, Set<Explanation.Inference>> existingInferences = existingExplanation.inferences();
//////
//////        // recursive call first
//////        for (Set<Explanation.Inference> inferences : existingInferences.values()) {
//////            for (Explanation.Inference inference : inferences) {
//////                Response.Answer inferenceAnswer = inference.answer();
//////                AnswerIndex index = new AnswerIndex(inferenceAnswer.sourceRequest().receiver(), inferenceAnswer.partialAnswer());
//////            }
//////        }
////
////
////
////
////        Map<String, Set<Explanation.Inference>> newInferences = newAnswer.explanation().inferences();
////        existingExplanation.update(newInferences);
////
////        return existingAnswer;
////    }
//
//    public void recordRootAnswer() {
//        // TODO
//    }
//
//    private void createIndices(Derivations derivations) {
//
//    }
//
    @Override
    protected void exception(final Exception e) {
        LOG.error(e.toString());
    }

    public void recordTree(Actor<? extends ExecutionActor<?>> self, List<Long> conceptMap, Response.Answer answer) {

    }
//
//
//    static class AnswerIndex {
//        private final Actor<Concludable> deduplicator;
//        private List<Long> conceptMap;
////        private final Actor<? extends ExecutionActor<?>> producer;
//
//        public AnswerIndex(final Actor<Concludable> deduplicator, final List<Long> conceptMap) {
//            this.deduplicator = deduplicator;
////            this.conceptMap = this.conceptMap;
////            this.producer = producer;
//        }
////
////        @Override
////        public boolean equals(final Object o) {
////            if (this == o) return true;
////            if (o == null || getClass() != o.getClass()) return false;
////            final AnswerIndex that = (AnswerIndex) o;
////            return deduplicator.equals(that.deduplicator) &&
////                    conceptMap.equals(that.conceptMap) &&
////                    producer.equals(that.producer);
////        }
////
////        @Override
////        public int hashCode() {
////            return Objects.hash(deduplicator, conceptMap, producer);
////        }
//    }
}
