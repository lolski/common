package grakn.common.poc.reasoning.model;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static grakn.common.collection.Collections.map;
import static grakn.common.collection.Collections.pair;

public class Explanation {
    public static final Explanation EMPTY = new Explanation(map());

    private Map<String, Set<Inference>> inferences;

    public Explanation(Map<String, Set<Inference>> inferences) {
        this.inferences = map(inferences);
    }

    public Explanation withInference(String pattern, Set<Inference> inference) {
        Map<String, Set<Inference>> copiedInferences = new HashMap<>(inferences);
        copiedInferences.put(pattern, inference);
        return new Explanation(copiedInferences);
    }

    public void update(Map<String, Set<Inference>> newInferences) {
        Map<String, Set<Inference>> copiedInferences = new HashMap<>(inferences);
        copiedInferences.putAll(newInferences);
        inferences = copiedInferences;
    }


    public Map<String, Set<Inference>> inferences() {
        return this.inferences;
    }

    @Override
    public String toString() {
        return "Explanation{" +
                "inferences=" + inferences +
                '}';
    }

    public static class Inference {
        @Nullable
        private String ruleName;
        private String ruleWhen;
        private Response.Answer answer;

        public Inference(Response.Answer answer, String ruleWhen, @Nullable String ruleName) {
            this.answer = answer;
            this.ruleWhen = ruleWhen;
            this.ruleName = ruleName;
        }

        public Response.Answer answer() {
            return answer;
        }

        @Override
        public String toString() {
            return "Inference{" +
                    "ruleName='" + ruleName + '\'' +
                    ", ruleWhen='" + ruleWhen + '\'' +
                    ", answer=" + answer +
                    '}';
        }
    }
}
