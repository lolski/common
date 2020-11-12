package grakn.common.poc.reasoning.execution;


import grakn.common.concurrent.actor.Actor;

import java.util.List;
import java.util.Objects;

public class Request {
    private Actor<? extends ExecutionActor<?>> sender;
    private Actor<? extends ExecutionActor<?>> receiver;
    private final List<Long> partialAnswer;
    private final List<Object> constraints;
    private final List<Object> unifiers;

    public Request(Actor<? extends ExecutionActor<?>> sender,
                   Actor<? extends ExecutionActor<?>> receiver,
                   List<Long> partialAnswer,
                   List<Object> constraints,
                   List<Object> unifiers) {
        this.sender = sender;
        this.receiver = receiver;
        this.partialAnswer = partialAnswer;
        this.constraints = constraints;
        this.unifiers = unifiers;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Request request = (Request) o;
        return Objects.equals(receiver, request.receiver) &&
                Objects.equals(partialAnswer, request.partialAnswer()) &&
                Objects.equals(constraints, request.constraints()) &&
                Objects.equals(unifiers, request.unifiers());
    }

    @Override
    public int hashCode() {
        return Objects.hash(receiver, partialAnswer, constraints, unifiers);
    }

    public Actor<? extends ExecutionActor<?>> sender() {
        return sender;
    }

    public Actor<? extends ExecutionActor<?>> receiver() {
        return receiver;
    }

    public List<Long> partialAnswer() {
        return partialAnswer;
    }

    public List<Object> constraints() {
        return constraints;
    }

    public List<Object> unifiers() {
        return unifiers;
    }

}