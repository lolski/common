package grakn.common.poc.reasoning;

import grakn.common.collection.Either;
import grakn.common.concurrent.actor.Actor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public abstract class ExecutionActor<T extends ExecutionActor<T>> extends Actor.State<T>{
    final Logger LOG;
    String name;

    private final LinkedBlockingQueue<Long> responses;
    private final ActorRegistry actorRegistry;
    private final Map<Request, ResponseProducer> responseProducers;
    private final Map<Request, Request> requestRouter;

    protected ExecutionActor(final Actor<T> self, final ActorRegistry actorRegistry, final String name, LinkedBlockingQueue<Long> responses) {
        super(self);
        LOG = LoggerFactory.getLogger(name);
        this.actorRegistry = actorRegistry;
        this.name = name;
        responseProducers = new HashMap<>();
        requestRouter = new HashMap<>();
        this.responses = responses;
    }

    protected ExecutionActor(final Actor<T> self, final ActorRegistry actorRegistry, final String name) {
        super(self);
        LOG = LoggerFactory.getLogger(name);
        this.actorRegistry = actorRegistry;
        this.name = name;
        responseProducers = new HashMap<>();
        requestRouter = new HashMap<>();
        this.responses = null;
    }


    abstract ResponseProducer createResponseProducer(final Request fromUpstream);

    abstract Either<Request, Response> receiveRequest(final Request fromUpstream, final ResponseProducer responseProducer);

    abstract Either<Request, Response> receiveAnswer(final Request fromUpstream, final Response.Answer fromDownstream, final ResponseProducer responseProducer);

    abstract Either<Request, Response> receiveExhausted(final Request fromUpstream, final Response.Exhausted fromDownstream, final ResponseProducer responseProducer);

    /*
    Handlers for messages sent into the execution actor that are dispatched via the actor model
     */
    public void executeReceiveRequest(final Request fromUpstream) {
        LOG.debug("Received fromUpstream in: " + name);

        ResponseProducer responseProducer = responseProducers.computeIfAbsent(fromUpstream, key -> createResponseProducer(fromUpstream));
        responseProducer.incrementRequestsFromUpstream();
        Either<Request, Response> action = receiveRequest(fromUpstream, responseProducer);
        if (action.isFirst()) requestFromDownstream(action.first(), fromUpstream, responseProducer);
        else respondToUpstream(action.second(), responseProducer);
    }

    void executeReceiveAnswer(final Response.Answer fromDownstream) {
        LOG.debug("Received fromDownstream response in: " + name);
        Request sentDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = requestRouter.get(sentDownstream);
        ResponseProducer responseProducer = responseProducers.get(fromUpstream);
        responseProducer.decrementRequestsToDownstream();
        Either<Request, Response> action = receiveAnswer(fromUpstream, fromDownstream, responseProducer);

        if (action.isFirst()) requestFromDownstream(action.first(), fromUpstream, responseProducer);
        else respondToUpstream(action.second(), responseProducer);
    }

    void executeReceiveExhausted(final Response.Exhausted fromDownstream) {
        LOG.debug("Received fromDownstream response in: " + name);
        Request sentDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = requestRouter.get(sentDownstream);
        ResponseProducer responseProducer = responseProducers.get(fromUpstream);
        responseProducer.decrementRequestsToDownstream();

        Either<Request, Response> action = receiveExhausted(fromUpstream, fromDownstream, responseProducer);

        if (action.isFirst()) requestFromDownstream(action.first(), fromUpstream,  responseProducer);
        else respondToUpstream(action.second(), responseProducer);

    }

    private void requestFromDownstream(Request request, Request fromUpstream, ResponseProducer responseProducer) {
        LOG.debug("Requesting from downstream in: " + name);
        // TODO we may overwrite if multiple identical requests are sent, when to clean up?
        requestRouter.put(request, fromUpstream);
        Actor<? extends ExecutionActor<?>> targetActor = request.plan().currentStep();
        responseProducer.incrementRequestsToDownstream();
        targetActor.tell(actor -> actor.executeReceiveRequest(request));
    }

    private void respondToUpstream(Response response, ResponseProducer responseProducer) {
        Actor<? extends ExecutionActor<?>> targetActor = response.plan().currentStep();
        if (targetActor == null) {
            assert responses != null : this + ": can't return answers because the user answers queue is null";
            if (response.isAnswer()) {
                Long mergedAnswer = response.asAnswer().partialAnswers.stream().reduce(0L, (acc, val) -> acc + val);
                LOG.debug("Writing Answer to output queue in: " + name);
                responses.add(mergedAnswer);
            } else {
                LOG.debug("Writing Exhausted to output queue in: " + name);
                responses.add(-1L);
            }
        } else {
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