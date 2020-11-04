package grakn.common.poc.reasoning;

import java.util.List;

interface Response {
    Request sourceRequest();

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

    }
}