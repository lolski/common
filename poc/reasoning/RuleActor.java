package grakn.common.poc.reasoning;

import grakn.common.concurrent.actor.Actor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RuleActor extends ReasoningActor<RuleActor> {
    Logger LOG;

    private final Actor<ConjunctiveActor> whenActor;
    private final Map<Request, ResponseProducer> requestProducers;
    private final Map<Request, Request> requestRouter;
    private final String name;

    public RuleActor(final Actor<RuleActor> self, ActorManager manager, String ruleName, List<Long> when,
                     Long whenTraversalSize) throws InterruptedException {
        super(self);
        LOG = LoggerFactory.getLogger(RuleActor.class.getSimpleName() + "-" + ruleName);

        this.name = String.format("RuleActor[%s](pattern:%s)", ruleName, ruleName);
        whenActor = manager.createConjunctiveActor(when, whenTraversalSize);
        requestRouter = new HashMap<>();
        requestProducers = new HashMap<>();
    }

    @Override
    public void receiveRequest(final Request request) {
        assert request.path.atEnd() : "Rule receiving a request must be path terminal";

        if (!this.requestProducers.containsKey(request)) {
            this.requestProducers.put(request, new ResponseProducer(true));
        }

        ResponseProducer responseProducer = this.requestProducers.get(request);
        if (responseProducer.finished()) {
            respondDoneToRequester(request);
        } else {
            // TODO if we want batching, we increment by as many as are requested
            responseProducer.requestsFromUpstream++;

            if (responseProducer.requestsFromUpstream > responseProducer.requestsToDownstream + responseProducer.answers.size()) {
                respondAnswersToRequester(request, responseProducer);
            }

            if (responseProducer.requestsFromUpstream > responseProducer.requestsToDownstream + responseProducer.answers.size()) {
                if (!responseProducer.isDownstreamDone()) {
                    requestFromDownstream(request);
                }
            }
        }
    }

    @Override
    public void receiveAnswer(final Response.Answer answer) {
        LOG.debug("Received answer response in: " + name);
        Request request = answer.request();
        Request parentRequest = requestRouter.get(request);
        ResponseProducer responseProducer = requestProducers.get(parentRequest);
        responseProducer.requestsToDownstream--;
        respondAnswersToRequester(parentRequest, responseProducer);

        // TODO unify and materialise
    }

    @Override
    public void receiveDone(final Response.Done done) {
        LOG.debug("Received done response in: " + name);
        Request request = done.request();
        Request parentRequest = requestRouter.get(request);
        ResponseProducer responseProducer = requestProducers.get(parentRequest);
        responseProducer.requestsToDownstream--;

        responseProducer.setDownstreamDone();

        if (responseProducer.finished()) {
            respondDoneToRequester(parentRequest);
        } else {
            respondAnswersToRequester(parentRequest, responseProducer);
        }
    }

    @Override
    void requestFromDownstream(final Request request) {
        Path extendedPath = request.path.extend(whenActor);
        Path downstreamPath = extendedPath.moveDownstream();
        Request subrequest = new Request(
                downstreamPath,
                request.partialAnswers,
                request.constraints,
                request.unifiers
        );

        // TODO we may overwrite if multiple identical requests are sent, when to clean up?
        requestRouter.put(subrequest, request);

        LOG.debug("Requesting from downstream in: " + name);
        whenActor.tell(actor -> actor.receiveRequest(subrequest));
    }

    @Override
    void respondAnswersToRequester(final Request request, final ResponseProducer responseProducer) {
        // send as many answers as possible to requester
        for (int i = 0; i < Math.min(responseProducer.requestsFromUpstream, responseProducer.answers.size()); i++) {
            Long answer = responseProducer.answers.remove(0);
            Actor<? extends ReasoningActor<?>> requester = request.path.directUpstream();
            Path newPath = request.path.moveUpstream();
            Response.Answer responseAnswer = new Response.Answer(
                    request,
                    newPath,
                    Arrays.asList(answer),
                    request.constraints,
                    request.unifiers
            );

            LOG.debug("Responding answer to requester in: " + name);
            requester.tell((actor) -> actor.receiveAnswer(responseAnswer));
            responseProducer.requestsFromUpstream--;
        }
    }

    @Override
    void respondDoneToRequester(final Request request) {
        Actor<? extends ReasoningActor<?>> requester = request.path.directUpstream();
        Path newPath = request.path.moveUpstream();
        Response.Done responseDone = new Response.Done(request, newPath);
        LOG.debug("Responding Done to requester in: " + name);
        requester.tell((actor) -> actor.receiveDone(responseDone));
    }
}
