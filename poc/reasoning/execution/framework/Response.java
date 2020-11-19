package grakn.common.poc.reasoning.execution.framework;

import java.util.List;

import static grakn.common.collection.Collections.map;

public interface Response {
    Request sourceRequest();

    boolean isAnswer();
    boolean isExhausted();

    default Answer asAnswer() {
        throw new ClassCastException("Cannot cast " + this.getClass().getSimpleName() + " to " + Answer.class.getSimpleName());
    }

    default Exhausted asExhausted() {
        throw new ClassCastException("Cannot cast " + this.getClass().getSimpleName() + " to " + Exhausted.class.getSimpleName());
    }

    class Answer implements Response {
        private final Request sourceRequest;
        private final grakn.common.poc.reasoning.execution.framework.Answer answer;
        private final List<Object> unifiers;

        public Answer(Request sourceRequest,
                      grakn.common.poc.reasoning.execution.framework.Answer answer,
                      List<Object> unifiers) {
            this.sourceRequest = sourceRequest;
            this.answer = answer;
            this.unifiers = unifiers;
        }

        @Override
        public Request sourceRequest() {
            return sourceRequest;
        }

        public grakn.common.poc.reasoning.execution.framework.Answer answer() {
            return answer;
        }

        public List<Object> unifiers() {
            return unifiers;
        }

        @Override
        public boolean isAnswer() { return true; }

        @Override
        public boolean isExhausted() { return false; }

        @Override
        public Answer asAnswer() {
            return this;
        }

        @Override
        public String toString() {
            return "\nAnswer{" +
                    "\nsourceRequest=" + sourceRequest +
                    ",\nanswer=" + answer +
                    ",\nunifiers=" + unifiers +
                    "\n}\n";
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
        public Exhausted asExhausted() {
            return this;
        }


        @Override
        public String toString() {
            return "Exhausted{" +
                    "sourceRequest=" + sourceRequest +
                    '}';
        }
    }

}
