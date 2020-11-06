package grakn.common.poc.reasoning;

import java.util.List;
import java.util.Objects;

public class RuleTrigger {
    private final List<Long> partialAnswer;
    private final List<Object> constraints;

    public RuleTrigger(List<Long> partialAnswer, List<Object> constraints) {
        this.partialAnswer = partialAnswer;
        this.constraints = constraints;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RuleTrigger that = (RuleTrigger) o;
        return Objects.equals(partialAnswer, that.partialAnswer) &&
                Objects.equals(constraints, that.constraints);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partialAnswer, constraints);
    }
}
