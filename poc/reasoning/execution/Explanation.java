package grakn.common.poc.reasoning.execution;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static grakn.common.collection.Collections.map;

/*
Carry explanations across actors
NOTE: always shallow copy on sending an answer, as requests messages are shared
 */
public class Explanation {

    public static final Explanation EMPTY = new Explanation(map());

    private final Map<String, Set<Inference>> inferences;

    public Explanation(Map<String, Set<Inference>> inferences) {
        this.inferences = inferences;
    }

    public Explanation copy() {
        Map<String, Set<Inference>> copiedInferences = new HashMap<>(inferences);
        return new Explanation(copiedInferences);
    }

    public void addInference(String pattern, Set<Inference> inference) {
        assert !inferences.containsKey(pattern) : "cannot overwrite prexisting pattern inferences with new ones";
        inferences.put(pattern, inference);
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
