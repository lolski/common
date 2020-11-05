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

    abstract ResponseProducer createResponseProducer(final Request fromUpstream);

    abstract Either<Request, Response> receiveRequest(final Request fromUpstream, final ResponseProducer responseProducer);

    abstract Either<Request, Response> receiveAnswer(final Request fromUpstream, final Response.Answer fromDownstream, final ResponseProducer responseProducer);

    abstract Either<Request, Response> receiveExhausted(final Request fromUpstream, final Response.Exhausted fromDownstream, final ResponseProducer responseProducer);

    /*
    Handlers for messages sent into the execution actor that are dispatched via the actor model
     */
    void executeReceiveRequest(final Request fromUpstream) {
        LOG.debug("Received fromUpstream in: " + name);

        ResponseProducer responseProducer = responseProducers.computeIfAbsent(fromUpstream, key -> createResponseProducer(fromUpstream));
        responseProducer.incrementRequestsFromUpstream();
        Either<Request, Response> action = receiveRequest(fromUpstream, responseProducer);
        if (action.isFirst()) {
            LOG.debug("Requesting from downstream in: " + name);
            Request request = action.first();
            // TODO we may overwrite if multiple identical requests are sent, when to clean up?
            requestRouter.put(request, fromUpstream);
            Actor<? extends ExecutionActor<?>> targetActor = request.plan().currentStep();
            responseProducer.incrementRequestsToDownstream();
            targetActor.tell(actor -> actor.executeReceiveRequest(request));
        } else {
            Response response = action.second();
            Actor<? extends ExecutionActor<?>> targetActor = response.plan().currentStep();
            if (response.isAnswer()) {
                LOG.debug("Responding answer to upstream from actor: " + name);
                targetActor.tell(actor -> actor.executeReceiveAnswer(response.asAnswer()));
            } else if (response.isExhausted()) {
                LOG.debug("Responding Exhausted to upstream from actor: " + name);
                targetActor.tell(actor -> actor.executeReceiveExhausted(response.asExhausted()));
            } else {
                throw new RuntimeException(("Unknown message type " + response.getClass().getSimpleName()));
            }
            responseProducer.decrementRequestsFromUpstream();
        }
    }

    void executeReceiveAnswer(final Response.Answer fromDownstream) {
        LOG.debug("Received fromDownstream response in: " + name);
        Request sentDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = requestRouter.get(sentDownstream);
        ResponseProducer responseProducer = responseProducers.get(fromUpstream);
        responseProducer.decrementRequestsToDownstream();
        Either<Request, Response> action = receiveAnswer(fromUpstream, fromDownstream ,responseProducer);
        if (action.isFirst()) {
            LOG.debug("Requesting from downstream in: " + name);
            Request request = action.first();
            // TODO we may overwrite if multiple identical requests are sent, when to clean up?
            requestRouter.put(request, fromUpstream);
            Actor<? extends ExecutionActor<?>> targetActor = request.plan().currentStep();
            responseProducer.incrementRequestsToDownstream();
            targetActor.tell(actor -> actor.executeReceiveRequest(request));
        } else {
            Response response = action.second();
            Actor<? extends ExecutionActor<?>> targetActor = response.plan().currentStep();
            LOG.debug("Responding answer to upstream from actor: " + name);
            responseProducer.decrementRequestsFromUpstream();
            targetActor.tell(actor -> actor.executeReceiveAnswer(response.asAnswer()));
        }
    }

    void executeReceiveExhausted(final Response.Exhausted fromDownstream) {
        LOG.debug("Received fromDownstream response in: " + name);
        Request sentDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = requestRouter.get(sentDownstream);
        ResponseProducer responseProducer = responseProducers.get(fromUpstream);
        responseProducer.decrementRequestsToDownstream();

        Either<Request, Response> action = receiveExhausted(fromUpstream, fromDownstream, responseProducer);
        if (action.isFirst()) {
            LOG.debug("Requesting from downstream in: " + name);
            Request request = action.first();
            // TODO we may overwrite if multiple identical requests are sent, when to clean up?
            requestRouter.put(request, fromUpstream);
            Actor<? extends ExecutionActor<?>> targetActor = request.plan().currentStep();
            responseProducer.incrementRequestsToDownstream();
            targetActor.tell(actor -> actor.executeReceiveRequest(request));
        } else {
            Response response = action.second();
            Actor<? extends ExecutionActor<?>> targetActor = response.plan().currentStep();
            if (response.isAnswer()) {
                LOG.debug("Responding answer to upstream from actor: " + name);
                targetActor.tell(actor -> actor.executeReceiveAnswer(response.asAnswer()));
            } else if (response.isExhausted()) {
                LOG.debug("Responding Exhausted to upstream from actor: " + name);
                targetActor.tell(actor -> actor.executeReceiveExhausted(response.asExhausted()));
            } else {
                throw new RuntimeException(("Unknown message type " + response.getClass().getSimpleName()));
            }
            responseProducer.decrementRequestsFromUpstream();
        }
    }
}
