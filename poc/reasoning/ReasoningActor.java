package grakn.common.poc.reasoning;

import grakn.common.concurrent.actor.Actor;

public abstract class ReasoningActor<T extends ReasoningActor<T>> extends Actor.State<T>{

    protected ReasoningActor(final Actor<T> self) {
        super(self);
    }

    /*
    TODO
    we will lazily create the required downstream actors when we receive the first request
    this can be achieved by asking the manager to create the required actors, but cannot block
    the manager must notify this object, when the downstream actors are completed.
    This can set the actor's state to be `initialised = true`, meaning it can then process requests.
    In the meantime, the actor CAN accept requests, but must buffer them. When `initialised` is set to `true`,
    the buffered requests can be processed.
     */
    public abstract void receiveRequest(final Request request);

    public abstract void receiveAnswer(final Response.Answer answer);

    public abstract void receiveDone(final Response.Done done);

    abstract void requestFromDownstream(final Request request);

    abstract void respondAnswersToRequester(final Request request, final ResponseProducer responseProducer);

    abstract void respondDoneToRequester(final Request request);
}
