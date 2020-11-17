package grakn.common.poc.reasoning;

import java.util.List;

public interface Response {
    Request sourceRequest();

    boolean isAnswer();
    boolean isExhausted();

    default Response.Answer asAnswer() {
        throw new ClassCastException("Cannot cast " + this.getClass().getSimpleName() + " to " + Response.Answer.class.getSimpleName());
    }

    default Response.Exhausted asExhausted() {
        throw new ClassCastException("Cannot cast " + this.getClass().getSimpleName() + " to " + Response.Exhausted.class.getSimpleName());
    }

    class Answer implements Response {
        private final Request sourceRequest;
        private final List<Long> partialAnswer;
        private final List<Object> constraints;
        private final List<Object> unifiers;

        private String patternAnswered;
        private final Explanation explanation;

        public Answer(Request sourceRequest,
                      List<Long> partialAnswer,
                      List<Object> constraints,
                      List<Object> unifiers,
                      String patternAnswered,
                      Explanation explanation) {
            this.sourceRequest = sourceRequest;
            this.partialAnswer = partialAnswer;
            this.constraints = constraints;
            this.unifiers = unifiers;
            this.patternAnswered = patternAnswered;
            this.explanation = explanation;
        }

        @Override
        public Request sourceRequest() {
            return sourceRequest;
        }

        public List<Long> partialAnswer() {
            return partialAnswer;
        }

        public List<Object> constraints() {
            return constraints;
        }

        public List<Object> unifiers() {
            return unifiers;
        }

        public Explanation explanation() {
            return explanation;
        }

        @Override
        public boolean isAnswer() { return true; }

        @Override
        public boolean isExhausted() { return false; }

        @Override
        public Response.Answer asAnswer() {
            return this;
        }

        @Override
        public String toString() {
            return "\nAnswer{" +
                    "\nsourceRequest=" + sourceRequest +
                    ",\n partialAnswer=" + partialAnswer +
                    ",\n constraints=" + constraints +
                    ",\n unifiers=" + unifiers +
                    ",\n patternAnswered=" + patternAnswered +
                    ",\n explanation=" + explanation +
                    '}';
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
        public Response.Exhausted asExhausted() {
            return this;
        }
    }
}