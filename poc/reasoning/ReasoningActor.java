package grakn.common.poc.reasoning;

import grakn.common.concurrent.actor.Actor;

public abstract class ReasoningActor<T extends ReasoningActor<T>> extends Actor.State<T>{

    protected ReasoningActor(final Actor<T> self) {
        super(self);
    }

    abstract void receiveRequest(final Request request);

    abstract void receiveAnswer(final Response.Answer answer);

    abstract void receiveDone(final Response.Done done);

}
