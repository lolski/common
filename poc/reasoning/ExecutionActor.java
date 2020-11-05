package grakn.common.poc.reasoning;

import grakn.common.collection.Either;
import grakn.common.concurrent.actor.Actor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class ExecutionActor<T extends ExecutionActor<T>> extends Actor.State<T>{
    final Logger LOG;
    String name;

    private final ActorRegistry actorRegistry;
    private final Map<Request, ResponseProducer> responseProducers;
    private final Map<Request, Request> requestRouter;

    protected ExecutionActor(final Actor<T> self, final ActorRegistry actorRegistry, final String name) {
        super(self);
        LOG = LoggerFactory.getLogger(name);
        this.actorRegistry = actorRegistry;
        this.name = name;
        responseProducers = new HashMap<>();
        requestRouter = new HashMap<>();
    }

    public abstract Either<Request, Response> receiveRequest(final Request fromUpstream);

    public abstract Either<Request, Response.Answer> receiveAnswer(final Response.Answer fromDownstream);

    public abstract Either<Request, Response.Exhausted> receiveExhausted(final Response.Exhausted fromDownstream);

    abstract void requestFromAvailableDownstream(final Request request);

    abstract void respondAnswerToUpstream(final Request request,
                                          final Plan plan,
                                          final List<Long> partialAnswers, // TODO: this should be a Map<Variable, Long> (every variable has one answer)
                                          final List<Object> constraints,
                                          final List<Object> unifiers,
                                          final ResponseProducer responseProducer,
                                          final Actor<? extends ExecutionActor<?>> upstream);

    abstract void respondExhaustedToUpstream(final Request request, final Plan responsePlan);

    abstract ResponseProducer createResponseProducer(final Request fromUpstream);

    /*
    Handlers for messages sent into the execution actor that are dispatched via the actor model
     */
    void executeReceiveRequest(final Request fromUpstream) {
        LOG.debug("Received fromUpstream in: " + name);

        ResponseProducer responseProducer = responseProducers.computeIfAbsent(fromUpstream, key -> createResponseProducer(fromUpstream));

        Either<Request, Response> requestResponse = receiveRequest(fromUpstream);
        if (requestResponse.isFirst()) {
            Request request = requestResponse.first();
            // TODO we may overwrite if multiple identical requests are sent, when to clean up?
            requestRouter.put(request, fromUpstream);
            responseProducer.incrementRequestsToDownstream();
            Actor<? extends ExecutionActor<?>> targetActor = request.plan().currentStep();
            LOG.debug("Requesting from downstream in: " + name);
            targetActor.tell(actor -> actor.executeReceiveRequest(request));
        } else {
            // TODO resume here
            Response response = requestResponse.second();
            Actor<? extends ExecutionActor<?>> targetActor = response.plan().currentStep();
            if (response.isAnswer()) {
                targetActor.tell(actor -> actor.executeReceiveAnswer(response.asAnswer()));
            } else if (response.isExhausted()) {
                targetActor.tell(actor -> actor.executeReceiveExhausted(response.asExhausted()));
            } else {
                throw new RuntimeException(("Unknown response type " + response.getClass().getSimpleName()));
            }
        }
    }

    void executeReceiveAnswer(final Response.Answer fromDownstream) {

    }

    void executeReceiveExhausted(final Response.Exhausted fromDownstream) {

    }
}
