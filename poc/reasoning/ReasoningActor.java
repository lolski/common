package grakn.common.poc.reasoning;

import grakn.common.concurrent.actor.Actor;

public abstract class ReasoningActor<T extends ReasoningActor<T>> extends Actor.State<T>{

    protected ReasoningActor(final Actor<T> self) {
        super(self);
    }

    public abstract void receiveRequest(final Request request);

    public abstract void receiveAnswer(final Response.Answer answer);

    public abstract void receiveDone(final Response.Done done);

    abstract void requestFromDownstream(final Request request);

    abstract void respondAnswersToRequester(final Request request, final ResponseProducer responseProducer);

    abstract void respondDoneToRequester(final Request request);
}
