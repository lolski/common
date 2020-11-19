package grakn.common.poc.reasoning.execution.framework;

import grakn.common.collection.Either;
import grakn.common.concurrent.actor.Actor;
import grakn.common.poc.reasoning.execution.Registry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import static grakn.common.collection.Collections.map;

public abstract class ExecutionActor<T extends ExecutionActor<T>> extends Actor.State<T> {
    private static final Logger LOG = LoggerFactory.getLogger(ExecutionActor.class);

    protected String name;
    private boolean isInitialised;
    @Nullable
    private final LinkedBlockingQueue<Response> responses;
    private final Map<Request, ResponseProducer> responseProducers;
    private final Map<Request, Request> requestRouter;

    public ExecutionActor(Actor<T> self, String name) {
        this(self, name, null);
    }

    public ExecutionActor(Actor<T> self, String name, @Nullable LinkedBlockingQueue<Response> responses) {
        super(self);
        this.name = name;
        isInitialised = false;
        this.responses = responses;
        responseProducers = new HashMap<>();
        requestRouter = new HashMap<>();
    }

    public String name() { return name; }

    protected abstract ResponseProducer createResponseProducer(Request fromUpstream);

    protected abstract void initialiseDownstreamActors(Registry registry);

    protected abstract Either<Request, Response> receiveRequest(Request fromUpstream, ResponseProducer responseProducer);

    protected abstract Either<Request, Response> receiveAnswer(Request fromUpstream, Response.Answer fromDownstream, ResponseProducer responseProducer);

    protected abstract Either<Request, Response> receiveExhausted(Request fromUpstream, Response.Exhausted fromDownstream, ResponseProducer responseProducer);

    /*
     *
     * Handlers for messages sent into the execution actor that are dispatched via the actor model.
     *
     */
    public void executeReceiveRequest(Request fromUpstream, Registry registry) {
        LOG.debug("{}: Receiving a new Request from upstream: {}", name, fromUpstream);
        if (!isInitialised) {
            LOG.debug(name + ": initialising downstream actors");
            initialiseDownstreamActors(registry);
            isInitialised = true;
        }

        ResponseProducer responseProducer = responseProducers.computeIfAbsent(fromUpstream, key -> {
            LOG.debug("{}: Creating a new ResponseProducer for the given Request: {}", name, fromUpstream);
            return createResponseProducer(fromUpstream);
        });
        Either<Request, Response> action = receiveRequest(fromUpstream, responseProducer);
        if (action.isFirst()) requestFromDownstream(action.first(), fromUpstream, registry);
        else respondToUpstream(action.second(), registry);
    }

    void executeReceiveAnswer(Response.Answer fromDownstream, Registry registry) {
        LOG.debug("{}: Receiving a new Answer from downstream: {}", name, fromDownstream);
        Request sentDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = requestRouter.get(sentDownstream);
        ResponseProducer responseProducer = responseProducers.get(fromUpstream);
        Either<Request, Response> action = receiveAnswer(fromUpstream, fromDownstream, responseProducer);

        if (action.isFirst()) requestFromDownstream(action.first(), fromUpstream, registry);
        else respondToUpstream(action.second(), registry);
    }

    void executeReceiveExhausted(Response.Exhausted fromDownstream, Registry registry) {
        LOG.debug("{}: Receiving a new Exhausted from downstream: {}", name, fromDownstream);
        Request sentDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = requestRouter.get(sentDownstream);
        ResponseProducer responseProducer = responseProducers.get(fromUpstream);

        Either<Request, Response> action = receiveExhausted(fromUpstream, fromDownstream, responseProducer);

        if (action.isFirst()) requestFromDownstream(action.first(), fromUpstream, registry);
        else respondToUpstream(action.second(), registry);

    }

    /*
     *
     * Helper method private to this class.
     *
     * */
    private void requestFromDownstream(Request request, Request fromUpstream, Registry registry) {
        LOG.debug("{} : Sending a new Request in order to request for an answer from downstream: {}", name, request);
        // TODO we may overwrite if multiple identical requests are sent, when to clean up?
        requestRouter.put(request, fromUpstream);
        Actor<? extends ExecutionActor<?>> receiver = request.receiver();
        receiver.tell(actor -> actor.executeReceiveRequest(request, registry));
    }

    private void respondToUpstream(Response response, Registry registry) {
        LOG.debug("{}: Sending a new Response to respond with an answer to upstream: {}", name, response);
        Actor<? extends ExecutionActor<?>> receiver = response.sourceRequest().sender();
        if (receiver == null) {
            assert responses != null : this + ": can't return answers because the user answers queue is null";
            LOG.debug("{}: Writing a new Response to output queue", name);
            responses.add(response);
        } else {
            if (response.isAnswer()) {
                LOG.debug("{} : Sending a new Response.Answer to upstream", name);
                receiver.tell(actor -> actor.executeReceiveAnswer(response.asAnswer(), registry));
            } else if (response.isExhausted()) {
                LOG.debug("{}: Sending a new Response.Exhausted to upstream", name);
                receiver.tell(actor -> actor.executeReceiveExhausted(response.asExhausted(), registry));
            } else {
                throw new RuntimeException(("Unknown message type " + response.getClass().getSimpleName()));
            }
        }
    }

}
