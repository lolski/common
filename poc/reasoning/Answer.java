package grakn.common.poc.reasoning;

import grakn.common.collection.Pair;

import java.util.Map;
import java.util.stream.Stream;

public class Answer {

    // for retrieving explanations from the server
    private int queryId;

    Map<String, String> idMap;
    private final String pattern;

    public Answer(Map<String, String> idMap, String pattern, int queryId) {
        this.idMap = idMap;
        this.pattern = pattern;
        this.queryId = queryId;
    }

    public Stream<Map<String, Explanation>> getExplanations() {
        return tx.explanations(queryId, idMap);
    }

    public boolean isInferred() {
        // conceptually, may not be final impl
        return getExplanations() != null;
    }
}

class Explanation {
    String ruleName;
    String pattern;
    Answer answer;
    private Explanation(String ruleName, String pattern, Answer answer) {
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

