package grakn.common.poc.reasoning;

import grakn.common.concurrent.actor.Actor;

import java.util.List;

public abstract class ReasoningActor<T extends ReasoningActor<T>> extends Actor.State<T>{

    final ActorRegistry actorRegistry;

    protected ReasoningActor(final Actor<T> self, ActorRegistry actorRegistry) {
        super(self);
        this.actorRegistry = actorRegistry;
    }

    public abstract void receiveRequest(final Request request);

    public abstract void receiveAnswer(final Response.Answer answer);

    public abstract void receiveDone(final Response.Done done);

    abstract void requestFromAvailableDownstream(final Request request);

    abstract void respondAnswersToUpstream(final Request request,
                                           final Plan plan,
                                           final List<Long> partialAnswers, // TODO: this should be a Map<Variable, Long> (every variable has one answer)
                                           final List<Object> constraints,
                                           final List<Object> unifiers,
                                           final ResponseProducer responseProducer,
                                           final Actor<? extends ReasoningActor<?>> upstream);

    abstract void respondDoneToUpstream(final Request request, Plan responsePlan);

}
