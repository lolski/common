package grakn.common.poc.reasoning;


import grakn.common.concurrent.actor.Actor;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class Path {
    List<Actor<? extends ReasoningActor<?>>> completePath;
    int current = 0;

    public Path(List<Actor<? extends ReasoningActor<?>>> initialDownstream) {
        completePath = new ArrayList<>(initialDownstream);
    }

    boolean atRoot() {
        return current == 0;
    }

    boolean atEnd() {
        return current == completePath.size() - 1;
    }

    public Actor<? extends ReasoningActor<?>> directUpstream() {
        return completePath.get(current - 1);
    }

    public Actor<? extends ReasoningActor<?>> directDownstream() {
        if (current == completePath.size() - 1) return null;
        return completePath.get(current + 1);
    }

    void addDownstream(Actor<AtomicActor> downstream) {
        completePath.add(downstream);
    }

    Path moveDownstream() {
        Path path = new Path(completePath);
        path.current = this.current + 1;
        assert path.current < path.completePath.size() : "Moved downstream past end of path";
        return path;
    }

    Path moveUpstream() {
        Path path = new Path(completePath);
        path.current = this.current - 1;
        assert path.current >= 0 : "Moved upstream past beginning of path";
        return path;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final Path path = (Path) o;
        return current == path.current && Objects.equals(completePath, path.completePath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(completePath, current);
    }
}
