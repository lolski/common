package grakn.common.poc.reasoning.execution.framework;

import grakn.common.concurrent.actor.Actor;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static grakn.common.collection.Collections.list;

public class Request {
    private final Path path;
    private final List<Long> partialConceptMap;
    private final List<Object> unifiers;
    private final ExecutionRecord partialExecutionRecord;

    public Request(Path path,
                   List<Long> partialConceptMap,
                   List<Object> unifiers,
                   ExecutionRecord partialExecutionRecord) {
        this.path = path;
        this.partialConceptMap = partialConceptMap;
        this.unifiers = unifiers;
        this.partialExecutionRecord = partialExecutionRecord;
    }

    public Path path() {
        return path;
    }

    @Nullable
    public Actor<? extends ExecutionActor<?>> sender() {
        if (path.path.size() < 2) {
            return null;
        }
        return path.path.get(path.path.size() - 2);
    }

    public Actor<? extends ExecutionActor<?>> receiver() {
        return path.path.get(path.path.size() - 1);
    }

    public List<Long> partialConceptMap() {
        return partialConceptMap;
    }

    public List<Object> unifiers() {
        return unifiers;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Request request = (Request) o;
        return Objects.equals(path, request.path) &&
                Objects.equals(partialConceptMap, request.partialConceptMap()) &&
                Objects.equals(unifiers, request.unifiers());
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, partialConceptMap, unifiers);
    }

    @Override
    public String toString() {
        return "Req(send=" + (sender() == null ? "<none>" : sender().state.name) + ", pAns=" + partialConceptMap + ")";
    }

    public ExecutionRecord partialResolutions() {
        return partialExecutionRecord;
    }

    public static class Path {
        final List<Actor<? extends ExecutionActor<?>>> path;

        public Path(Actor<? extends ExecutionActor<?>> sender) {
            this(list(sender));
        }

        private Path(List<Actor<? extends ExecutionActor<?>>> path) {
            assert !path.isEmpty() : "Path cannot be empty";
            this.path = path;
        }

        public Path append(Actor<? extends ExecutionActor<?>> actor) {
            List<Actor<? extends ExecutionActor<?>>> appended = new ArrayList<>(path);
            appended.add(actor);
            return new Path(appended);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Path path1 = (Path) o;
            return Objects.equals(path, path1.path);
        }

        @Override
        public int hashCode() {
            return Objects.hash(path);
        }
    }
}
