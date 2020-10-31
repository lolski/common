package grakn.common.poc.reasoning;

import java.util.List;

interface Response {
    Request request();

    class Done implements Response {
        private final Request request;
        final Path path;

        public Done(final Request request, final Path path) {
            this.request = request;
            this.path = path;
        }

        @Override
        public Request request() {
            return request;
        }

    }

    class Answer implements Response {
        final Path path;
        final List<Long> partialAnswers;
        final List<Object> constraints;
        final List<Object> unifiers;
        private final Request request;

        public Answer(final Request request,
                      final Path path,
                      final List<Long> partialAnswers,
                      final List<Object> constraints,
                      final List<Object> unifiers) {
            this.request = request;
            this.path = path;
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