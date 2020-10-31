package grakn.common.poc.reasoning;


import java.util.List;
import java.util.Objects;

public class Request {
    final Path path;
    final List<Long> partialAnswers;
    final List<Object> constraints;
    final List<Object> unifiers;

    public Request(Path path,
                   List<Long> partialAnswers,
                   List<Object> constraints,
                   List<Object> unifiers) {
        this.path = path;
        this.partialAnswers = partialAnswers;
        this.constraints = constraints;
        this.unifiers = unifiers;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Request request = (Request) o;
        return Objects.equals(path, request.path) &&
                Objects.equals(partialAnswers, request.partialAnswers) &&
                Objects.equals(constraints, request.constraints) &&
                Objects.equals(unifiers, request.unifiers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, partialAnswers, constraints, unifiers);
    }
}