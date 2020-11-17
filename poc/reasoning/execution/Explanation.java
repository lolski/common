package grakn.common.poc.reasoning.execution;

import grakn.common.poc.reasoning.execution.Response;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class Explanation {

    private final Map<String, Set<Inference>> inferences;

    public Explanation(Map<String, Set<Inference>> inferences) {
        this.inferences = inferences;
    }

    public Explanation copy() {
        Map<String, Set<Inference>> copiedInferences = new HashMap<>(inferences);
        return new Explanation(copiedInferences);
    }

    public static class Inference {
        @Nullable
        private String ruleName;
        private String pattern;
        private Response.Answer answer;

        public Inference(Response.Answer answer, String pattern, @Nullable String ruleName) {
            this.answer = answer;
            this.pattern = pattern;
            this.ruleName = ruleName;
        }
    }
}
