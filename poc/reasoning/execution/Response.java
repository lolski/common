package grakn.common.poc.reasoning.execution;

import grakn.common.concurrent.actor.Actor;

import java.util.List;

public interface Response {
    Request sourceRequest();
    Plan plan();
    Actor<? extends ExecutionActor<?>> downstream();

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
        private final Plan plan;
        private final Actor<? extends ExecutionActor<?>>  downstream;
        private final List<Long> partialAnswer;
        private final List<Object> constraints;
        private final List<Object> unifiers;

        public Answer(final Request sourceRequest,
                      final Plan plan,
                      final Actor<? extends ExecutionActor<?>> downstream,
                      final List<Long> partialAnswer,
                      final List<Object> constraints,
                      final List<Object> unifiers) {
            this.sourceRequest = sourceRequest;
            this.plan = plan;
            this.downstream = downstream;
            this.partialAnswer = partialAnswer;
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
        public Actor<? extends ExecutionActor<?>>  downstream() {
            return downstream;
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
        private Actor<? extends ExecutionActor<?>>  downstream;

        public Exhausted(final Request sourceRequest, final Plan plan, final Actor<? extends ExecutionActor<?>>  downstream) {
            this.sourceRequest = sourceRequest;
            this.plan = plan;
            this.downstream = downstream;
        }

        @Override
        public Request sourceRequest() {
            return sourceRequest;
        }

        @Override
        public Plan plan() { return plan; }

        @Override
        public Actor<? extends ExecutionActor<?>>  downstream() {
            return downstream;
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