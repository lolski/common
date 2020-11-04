package grakn.common.poc.reasoning;

import grakn.common.concurrent.actor.Actor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class ReasoningActor<T extends ReasoningActor<T>> extends Actor.State<T>{
    protected final ActorRegistry actorRegistry;
    protected final Map<Request, ResponseProducer> responseProducers;
    protected final Map<Request, Request> requestRouter;

    protected ReasoningActor(final Actor<T> self, ActorRegistry actorRegistry) {
        super(self);
        this.actorRegistry = actorRegistry;
        responseProducers = new HashMap<>();
        requestRouter = new HashMap<>();
    }

    public abstract void receiveRequest(final Request fromUpstream);

    public abstract void receiveAnswer(final Response.Answer fromDownstream);

    public abstract void receiveExhausted(final Response.Exhausted fromDownstream);

    abstract void requestFromAvailableDownstream(final Request request);

    abstract void respondAnswersToUpstream(final Request request,
                                           final Plan plan,
                                           final List<Long> partialAnswers, // TODO: this should be a Map<Variable, Long> (every variable has one answer)
                                           final List<Object> constraints,
                                           final List<Object> unifiers,
                                           final ResponseProducer responseProducer,
                                           final Actor<? extends ReasoningActor<?>> upstream);

    abstract void respondExhaustedToUpstream(final Request request, final Plan responsePlan);

}
