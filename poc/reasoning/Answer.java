package grakn.common.poc.reasoning;

import com.google.common.collect.Iterators;

import java.util.Map;
import java.util.stream.Stream;

public class Answer {

    // for retrieving explanations from the server
    private int queryId;

    Map<String, String> idMap;

    public Answer(Map<String, String> idMap, int queryId) {
        this.idMap = idMap;
        this.queryId = queryId;
    }

    // TODO how will a stream know in general to notify the backend its done?
    public Stream<Explanation> getExplanations() {
        return tx.explanations(queryId, idMap);
    }

    public boolean isInferred() {
        // conceptually, may not be final impl
        return getExplanations().findAny().isPresent();
    }
}

// can be assembled by the conjunction as it receives answers from the atomic. Will nest "inference" objects
// coming back from downstream
class Explanation {
    Map<String, Inference> inferences;
    public Explanation(Map<String, Inference> inferences) {
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


/*

A1 Answer [friendship(x,z) and hasname(x,n), x = 1, z = 3, n = -1]
      |
{ friendship(x,z): Explanation[mutual-friendship-rule, friendship(x,y) -> A2, friendship(y, z) -> A3] }
                               /                          \
A2 Answer [friendship(x,y), x = 1, y = 2]             A3 Answer[friendship(y, z) -> y = 2, z = 3]



A1 Answer [friendship(x,z) and hasname(x,n), x = 1, z = 3, n = -1]
      |
{ friendship(x,z): Explanation[mutual-friendship-rule, friendship(x,y) -> A2, friendship(y, z) -> A3] ,
  hasname(x,n)   : Explanation[has-name,  ... -> A4]}
                               /                          \
A2 Answer [friendship(x,y), x = 1, y = 2]             A3 Answer[friendship(y, z) -> y = 2, z = 3]


 */

