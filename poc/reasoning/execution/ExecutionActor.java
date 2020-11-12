package grakn.common.poc.reasoning.execution;

import grakn.common.collection.Either;
import grakn.common.concurrent.actor.Actor;
import grakn.common.poc.reasoning.Registry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public abstract class ExecutionActor<T extends ExecutionActor<T>> extends Actor.State<T>{
    private static final Logger LOG = LoggerFactory.getLogger(ExecutionActor.class);

    String name;
    private boolean isInitialised;
    @Nullable
    private final LinkedBlockingQueue<Long> responses;
    private final Map<Request, ResponseProducer> responseProducers;
    private final Map<Request, Request> requestRouter;

    public ExecutionActor(final Actor<T> self, final String name) {
        this(self, name, null);
    }

    public ExecutionActor(final Actor<T> self, final String name, LinkedBlockingQueue<Long> responses) {
        super(self);
        this.name = name;
        isInitialised = false;
        this.responses = responses;
        responseProducers = new HashMap<>();
        requestRouter = new HashMap<>();
    }

    protected abstract ResponseProducer createResponseProducer(final Request fromUpstream);

    protected abstract void initialiseDownstreamActors(Registry registry);

    protected abstract Either<Request, Response> receiveRequest(final Request fromUpstream, final ResponseProducer responseProducer);

    protected abstract Either<Request, Response> receiveAnswer(final Request fromUpstream, final Response.Answer fromDownstream, final ResponseProducer responseProducer);

    protected abstract Either<Request, Response> receiveExhausted(final Request fromUpstream, final Response.Exhausted fromDownstream, final ResponseProducer responseProducer);

    /*
     *
     * Handlers for messages sent into the execution actor that are dispatched via the actor model.
     *
     */
    public void executeReceiveRequest(final Request fromUpstream, final Registry registry) {
        LOG.debug(name + ": Received a new Request from upstream: " + fromUpstream);
        if (!isInitialised) {
            LOG.debug(name + ": initialising downstream actors");
            initialiseDownstreamActors(registry);
            isInitialised = true;
        }

        ResponseProducer responseProducer = responseProducers.computeIfAbsent(fromUpstream, key -> {
            LOG.debug(name + ": creating response producer for upstream: " + fromUpstream);
            return createResponseProducer(fromUpstream);
        });
        Either<Request, Response> action = receiveRequest(fromUpstream, responseProducer);
        if (action.isFirst()) requestFromDownstream(action.first(), fromUpstream, responseProducer, registry);
        else respondToUpstream(action.second(), responseProducer, registry);
    }

    void executeReceiveAnswer(final Response.Answer fromDownstream, final Registry registry) {
        LOG.debug(name + ": Received a new Answer from downstream");
        Request sentDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = requestRouter.get(sentDownstream);
        ResponseProducer responseProducer = responseProducers.get(fromUpstream);
        Either<Request, Response> action = receiveAnswer(fromUpstream, fromDownstream, responseProducer);

        if (action.isFirst()) requestFromDownstream(action.first(), fromUpstream, responseProducer, registry);
        else respondToUpstream(action.second(), responseProducer, registry);
    }

    void executeReceiveExhausted(final Response.Exhausted fromDownstream, final Registry registry) {
        LOG.debug(name + ": Received a new Exhausted from downstream");
        Request sentDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = requestRouter.get(sentDownstream);
        ResponseProducer responseProducer = responseProducers.get(fromUpstream);

        Either<Request, Response> action = receiveExhausted(fromUpstream, fromDownstream, responseProducer);

        if (action.isFirst()) requestFromDownstream(action.first(), fromUpstream,  responseProducer, registry);
        else respondToUpstream(action.second(), responseProducer, registry);

    }

    /*
     *
     * Helper method private to this class.
     *
     * */
    private void requestFromDownstream(final Request request, final Request fromUpstream, final ResponseProducer responseProducer, final Registry registry) {
        LOG.debug(name + ": requesting from downstream");
        // TODO we may overwrite if multiple identical requests are sent, when to clean up?
        requestRouter.put(request, fromUpstream);
        Actor<? extends ExecutionActor<?>> receiver = request.receiver();
        receiver.tell(actor -> actor.executeReceiveRequest(request, registry));
    }

    private void respondToUpstream(final Response response, final ResponseProducer responseProducer, final Registry registry) {
        Actor<? extends ExecutionActor<?>> receiver = response.sourceRequest().sender();
        if (receiver == null) {
            assert responses != null : this + ": can't return answers because the user answers queue is null";
            if (response.isAnswer()) {
                Long mergedAnswer = response.asAnswer().partialAnswer().stream().reduce(0L, (acc, val) -> acc + val);
                LOG.debug(name + ": Writing Answer to output queue");
                responses.add(mergedAnswer);
            } else {
                LOG.debug(name + ": Writing Exhausted to output queue");
                responses.add(-1L);
            }
        } else {
            if (response.isAnswer()) {
                LOG.debug(name + ": Responding to upstream with an Answer");
                receiver.tell(actor -> actor.executeReceiveAnswer(response.asAnswer(), registry));
            } else if (response.isExhausted()) {
                LOG.debug(name + ": Responding to upstream with an Exhausted");
                receiver.tell(actor -> actor.executeReceiveExhausted(response.asExhausted(), registry));
            } else {
                throw new RuntimeException(("Unknown message type " + response.getClass().getSimpleName()));
            }
        }
    }
}
