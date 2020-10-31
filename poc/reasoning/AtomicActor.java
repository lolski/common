package grakn.common.poc.reasoning;

import grakn.common.concurrent.actor.Actor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class AtomicActor extends ReasoningActor<AtomicActor> {
    Logger LOG;

    private final Map<Request, ResponseProducer> requestProducers;
    private final Map<Request, Request> requestRouter; // TODO note that this can be many to one, and is not catered for yet (ie. request followed the same request)
    private final String name;
    private final Long traversalPattern;
    private final long traversalIteratorLength;

    public AtomicActor(final Actor<AtomicActor> self, Long traversalPattern, final long traversalIteratorLength) {
        super(self);
        LOG = LoggerFactory.getLogger(ConjunctiveActor.class.getSimpleName() + "-" + traversalPattern);

        this.name = "AtomicActor(pattern: " + traversalPattern + ")";
        this.traversalPattern = traversalPattern;
        this.traversalIteratorLength = traversalIteratorLength;
        // the query pattern long represents some thing to pass to a traversal or resolve further
        requestProducers = new HashMap<>();
        requestRouter = new HashMap<>();
    }

    @Override
    public void receiveRequest(final Request request) {
        LOG.debug("Received request in: " + name);
        if (!this.requestProducers.containsKey(request)) {
            this.requestProducers.put(request, initialiseResponseProducer(request));
        }

        ResponseProducer responseProducer = this.requestProducers.get(request);

        if (responseProducer.finished()) {
            respondDoneToRequester(request);
        } else {
            // TODO if we want batching, we increment by as many as are requested
            responseProducer.requestsFromUpstream++;

            if (responseProducer.requestsFromUpstream > responseProducer.requestsToDownstream + responseProducer.answers.size()) {
                List<Long> answers = produceTraversalAnswers(responseProducer);
                responseProducer.answers.addAll(answers);
                respondAnswersToRequester(request, responseProducer);
            }

            if (responseProducer.requestsFromUpstream > responseProducer.requestsToDownstream + responseProducer.answers.size()) {
                if (!responseProducer.isDownstreamDone()) {
                    requestFromDownstream(request);
                }
            }
        }
    }

    /*
    When a receive and answer and pass the answer forward
    We map the request that generated the answer, to the originating request.
    We then copy the originating request, and clear the request path, as it must already have been satisfied.
     */
    @Override
    public void receiveAnswer(final Response.Answer answer) {
        LOG.debug("Received answer response in: " + name);
        Request request = answer.request();
        Request parentRequest = requestRouter.get(request);
        ResponseProducer responseProducer = requestProducers.get(parentRequest);

        List<Long> partialAnswers = answer.partialAnswers;
        Long mergedAnswers = partialAnswers.stream().reduce(0L, (acc, v) -> acc + v);
        registerTraversal(responseProducer, mergedAnswers);

        List<Long> answers = produceTraversalAnswers(responseProducer);
        responseProducer.answers.addAll(answers);
        respondAnswersToRequester(parentRequest, responseProducer);
    }

    @Override
    public void receiveDone(final Response.Done done) {
        LOG.debug("Received done response in: " + name);
        Request request = done.request();
        Request parentRequest = requestRouter.get(request);
        ResponseProducer responseProducer = requestProducers.get(parentRequest);

        responseProducer.setDownstreamDone();

        if (responseProducer.finished()) {
            respondDoneToRequester(parentRequest);
        } else {
            List<Long> answers = produceTraversalAnswers(responseProducer);
            responseProducer.answers.addAll(answers);
            respondAnswersToRequester(parentRequest, responseProducer);
        }
    }

    private void requestFromDownstream(final Request request) {
        // TODO open question - should downstream requests increment "requested" field?
        Actor<? extends ReasoningActor<?>> downstream = request.path.directDownstream();
        Path downstreamPath = request.path.moveDownstream();
        Request subrequest = new Request(
                downstreamPath,
                request.partialAnswers,
                request.constraints,
                request.unifiers
        );

        // TODO we may overwrite if multiple identical requests are sent, when to clean up?
        requestRouter.put(subrequest, request);

        LOG.debug("Requesting from downstream from: " + name);
        downstream.tell(actor -> actor.receiveRequest(subrequest));
    }

    private void respondAnswersToRequester(final Request request, final ResponseProducer responseProducer) {
        // send as many answers as possible to requester
        for (int i = 0; i < Math.min(responseProducer.requestsFromUpstream, responseProducer.answers.size()); i++) {
            Long answer = responseProducer.answers.remove(0);
            Actor<? extends ReasoningActor<?>> requester = request.path.directUpstream();
            Path newPath = request.path.moveUpstream();
            List<Long> newAnswers = new ArrayList<>(1 + request.partialAnswers.size());
            newAnswers.addAll(request.partialAnswers);
            newAnswers.add(answer);
            Response.Answer responseAnswer = new Response.Answer(
                    request,
                    newPath,
                    newAnswers,
                    request.constraints,
                    request.unifiers
            );

            LOG.debug("Responding answer to requester from actor: " + name);
            requester.tell((actor) -> actor.receiveAnswer(responseAnswer));
            responseProducer.requestsFromUpstream--;
        }
    }

    private void respondDoneToRequester(final Request request) {
        Actor<? extends ReasoningActor<?>> requester = request.path.directUpstream();
        Path newPath = request.path.moveUpstream();
        Response.Done responseDone = new Response.Done(request, newPath);
        LOG.debug("Responding Done to requester from actor: " + name);
        requester.tell((actor) -> actor.receiveDone(responseDone));
    }

    private List<Long> produceTraversalAnswers(final ResponseProducer responseProducer) {
        Iterator<Long> traversalProducer = responseProducer.getOneTraversalProducer();
        if (traversalProducer != null) {
            // TODO could do batch traverse, or retrieve answers from multiple traversals
            Long answer = traversalProducer.next();
            if (!traversalProducer.hasNext()) responseProducer.removeTraversalProducer(traversalProducer);
            answer += this.traversalPattern;
            return Arrays.asList(answer);
        }
        return Arrays.asList();
    }

    private ResponseProducer initialiseResponseProducer(final Request request) {
        boolean downstreamExists = request.path.directDownstream() != null;
        ResponseProducer responseProducer = new ResponseProducer(downstreamExists);

        if (responseProducer.isDownstreamDone()) {
            registerTraversal(responseProducer, 0L);
        }

        return responseProducer;
    }

    private void registerTraversal(ResponseProducer responseProducer, long partialAnswer) {
        Iterator<Long> traversal = (new MockTransaction(traversalIteratorLength, 1)).query(partialAnswer);
        responseProducer.addTraversalProducer(traversal);
    }
}





