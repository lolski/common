package grakn.common.poc.reasoning;

import java.util.Map;
import java.util.Set;

public class Answer {

    // for retrieving explanations from the server
    private int queryId;

    Map<String, String> idMap;

    public Answer(Map<String, String> idMap, int queryId) {
        this.idMap = idMap;
        this.queryId = queryId;
    }

    public Explanation getExplanation() {
        return tx.explanation(queryId, idMap);
    }

    public boolean isInferred() {
        return getExplanation() != null;
    }
}

// can be assembled by the conjunction as it receives answers from the atomic. Will nest "inference" objects
// coming back from downstream
class Explanation {
    Map<String, Set<Inference>> inferences;
    public Explanation(Map<String, Set<Inference>> inferences) {
        this.inferences = inferences;
    }
}

// eg could be built by an atomic receiving an answer from downstream
class Inference {
    String ruleName;
    String pattern;
    Answer answer;
    private Inference(String ruleName, String pattern, Answer answer) {
        this.ruleName = ruleName;
        this.pattern = pattern;
        this.answer = answer;
    }
}

