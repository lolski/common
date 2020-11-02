package grakn.common.poc.reasoning;

import java.util.List;

interface Response {
    Request request();

    class Done implements Response {
        private final Request request;
        final Plan plan;

        public Done(final Request request, final Plan plan) {
            this.request = request;
            this.plan = plan;
        }

        @Override
        public Request request() {
            return request;
        }

    }

    class Answer implements Response {
        final Plan plan;
        final List<Long> partialAnswers;
        final List<Object> constraints;
        final List<Object> unifiers;
        private final Request request;

        public Answer(final Request request,
                      final Plan plan,
                      final List<Long> partialAnswers,
                      final List<Object> constraints,
                      final List<Object> unifiers) {
            this.request = request;
            this.plan = plan;
            this.partialAnswers = partialAnswers;
            this.constraints = constraints;
            this.unifiers = unifiers;
        }

        @Override
        public Request request() {
            return request;
        }

    }
}