package grakn.common.poc.reasoning;

import grakn.common.concurrent.actor.Actor;
import grakn.common.poc.reasoning.model.Explanation;
import grakn.common.poc.reasoning.model.Request;
import grakn.common.poc.reasoning.model.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class ExplanationRecorder extends Actor.State<ExplanationRecorder> {
    private static final Logger LOG = LoggerFactory.getLogger(ExplanationRecorder.class);

    Map<List<Long>, Response.Answer> rootAnswers;
    Map<AnswerIndex, Response.Answer> answerIndex;

    protected ExplanationRecorder(final Actor<ExplanationRecorder> self) {
        super(self);
    }

    public void recordRootAnswer() {
        // TODO
    }

    public void recordDuplicateAnswer(Request request, List<Long> partialAnswer, Explanation explanation) {
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
        private Request.Path path;
        private List<Long> answer;

        AnswerIndex(Request.Path path, List<Long> answer) {
            this.path = path;
            this.answer = answer;
        }

    }
}
