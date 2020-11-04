package grakn.common.poc.reasoning;


import java.util.List;
import java.util.Objects;

// TODO make immutable, so we can cache hashes
public class Request {
    final Plan plan;
    final List<Long> partialAnswers;
    final List<Object> constraints;
    final List<Object> unifiers;

    public Request(Plan plan,
                   List<Long> partialAnswers,
                   List<Object> constraints,
                   List<Object> unifiers) {
        this.plan = plan;
        this.partialAnswers = partialAnswers;
        this.constraints = constraints;
        this.unifiers = unifiers;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Request request = (Request) o;
        return Objects.equals(plan, request.plan) &&
                Objects.equals(partialAnswers, request.partialAnswers) &&
                Objects.equals(constraints, request.constraints) &&
                Objects.equals(unifiers, request.unifiers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(plan, partialAnswers, constraints, unifiers);
    }
}