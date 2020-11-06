package grakn.common.poc.reasoning;


import java.util.List;
import java.util.Objects;

public class Request {
    private final Plan plan;
    final List<Long> partialAnswer;
    final List<Object> constraints;
    final List<Object> unifiers;

    public Request(Plan plan,
                   List<Long> partialAnswer,
                   List<Object> constraints,
                   List<Object> unifiers) {
        this.plan = plan;
        this.partialAnswer = partialAnswer;
        this.constraints = constraints;
        this.unifiers = unifiers;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Request request = (Request) o;
        return Objects.equals(plan, request.plan) &&
                Objects.equals(partialAnswer, request.partialAnswer) &&
                Objects.equals(constraints, request.constraints) &&
                Objects.equals(unifiers, request.unifiers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(plan, partialAnswer, constraints, unifiers);
    }

    public Plan plan() { return plan; }
}