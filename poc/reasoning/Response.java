package grakn.common.poc.reasoning;

import java.util.List;

interface Response {
    Request sourceRequest();
    Plan plan();

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
        final Plan plan;
        final List<Long> partialAnswers;
        final List<Object> constraints;
        final List<Object> unifiers;

        public Answer(final Request sourceRequest,
                      final Plan plan,
                      final List<Long> partialAnswers,
                      final List<Object> constraints,
                      final List<Object> unifiers) {
            this.sourceRequest = sourceRequest;
            this.plan = plan;
            this.partialAnswers = partialAnswers;
            this.constraints = constraints;
            this.unifiers = unifiers;
        }

        @Override
        public Request sourceRequest() {
            return sourceRequest;
        }

        @Override
        public Plan plan() { return plan; }

        @Override
        public boolean isAnswer() { return true; }

        @Override
        public boolean isExhausted() { return false; }

        @Override
        public Response.Answer asAnswer() {
            return this;
        }
    }

    class Exhausted implements Response {
        private final Request sourceRequest;
        final Plan plan;

        public Exhausted(final Request sourceRequest, final Plan plan) {
            this.sourceRequest = sourceRequest;
            this.plan = plan;
        }

        @Override
        public Request sourceRequest() {
            return sourceRequest;
        }

        @Override
        public Plan plan() { return plan; }

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