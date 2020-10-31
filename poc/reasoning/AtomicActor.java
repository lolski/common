package grakn.common.poc.reasoning;

import grakn.common.concurrent.actor.Actor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class AtomicActor extends Actor.State<AtomicActor> {
    Logger LOG;

    private final Map<Request, ResponseProducer> requestProducers;
    private final Map<Request, Request> requestRouter; // TODO note that this can be many to one, and is not catered for yet (ie. request followed the same request)
    private final String name;
    private final Long traversalPattern;
    private final long traversalIteratorLength;
    @Nullable private final LinkedBlockingQueue<Long> responses;

    public AtomicActor(final Actor<AtomicActor> self, final String name, Long traversalPattern, final long traversalIteratorLength, final @Nullable LinkedBlockingQueue<Long> responses) {
        super(self);
        LOG = LoggerFactory.getLogger(AtomicActor.class + "-" + name);
        this.name = name;
        this.traversalPattern = traversalPattern;
        this.traversalIteratorLength = traversalIteratorLength;
        this.responses = responses;
        // the query pattern long represents some thing to pass to a traversal or resolve further
        requestProducers = new HashMap<>();
        requestRouter = new HashMap<>();
    }

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
    public void receiveAnswer(final Response.Answer answer) {
        LOG.debug("Received answer response in: " + name);
        Request sourceSubRequest = answer.request();
        Request parentRequest = requestRouter.get(sourceSubRequest);
        ResponseProducer responseProducer = requestProducers.get(parentRequest);

        List<Long> partialAnswers = answer.partialAnswers;
        Long mergedAnswers = partialAnswers.stream().reduce(0L, (acc, v) -> acc + v);
        registerTraversal(responseProducer, mergedAnswers);

        List<Long> answers = produceTraversalAnswers(responseProducer);
        responseProducer.answers.addAll(answers);
        respondAnswersToRequester(parentRequest, responseProducer);
    }

    public void receiveDone(final Response.Done done) {
        LOG.debug("Received done response in: " + name);
        Request sourceSubRequest = done.request();
        Request parentRequest = requestRouter.get(sourceSubRequest);
        ResponseProducer responseProducer = requestProducers.get(parentRequest);

        responseProducer.setDownstreamDone();

        if (responseProducer.finished()) {
            respondDoneToRequester(parentRequest);
        } else {
            List<Long> answers = produceTraversalAnswers(responseProducer);
            responseProducer.answers.addAll(answers);
            respondAnswersToRequester(parentRequest, responseProducer);
        }



        /*
        TODO: major flaw here is that when we get a DONE, we have fewer messages dispatched which should have
        TODO: lead to answers to the original request. To compensate, we should "retry" getting answers
        TODO: either from another actor, or a local traversal from the list of local traversals
         */
    }

    private void requestFromDownstream(final Request request) {
        // TODO open question - should downstream requests increment "requested" field?
        Actor<AtomicActor> downstream = request.path.directDownstream();
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

    /*
    when we respond to a request with an answer, must trim requestPath
     */
    private void respondAnswersToRequester(final Request request, final ResponseProducer responseProducer) {
        // send as many answers as possible to requester
        for (int i = 0; i < Math.min(responseProducer.requestsFromUpstream, responseProducer.answers.size()); i++) {
            Long answer = responseProducer.answers.remove(0);
            if (request.path.atRoot()) {
                // base case - how to return from Actor model
                assert responses != null : this + ": can't return answers because the user answers queue is null";
                LOG.debug("Saving answer to output queue in actor: " + name);
                responses.add(answer);
            } else {
                Actor<AtomicActor> requester = request.path.directUpstream();
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
            }
            responseProducer.requestsFromUpstream--;
        }
    }

    private void respondDoneToRequester(final Request request) {
        if (request.path.atRoot()) {
            // base case - how to return from Actor model
            assert responses != null : this + ": can't return answers because the user answers queue is null";
            LOG.debug("Sending DONE response to output from actor: " + name);
            responses.add(-1L);
        } else {
            Actor<AtomicActor> requester = request.path.directUpstream();
            Path newPath = request.path.moveUpstream();
            Response.Done responseDone = new Response.Done(request, newPath);
            LOG.debug("Responding Done to requester from actor: " + name);
            requester.tell((actor) -> actor.receiveDone(responseDone));
        }
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

        if (!responseProducer.isDownstreamDone()) {
            registerTraversal(responseProducer, 0L);
        }

        return responseProducer;
    }

    private void registerTraversal(ResponseProducer responseProducer, long partialAnswer) {
        Iterator<Long> traversal = (new MockTransaction(traversalIteratorLength, 1)).query(partialAnswer);
        responseProducer.addTraversalProducer(traversal);
    }
}

class ResponseProducer {
    List<Iterator<Long>> traversalProducers;
    boolean downstreamDone;
    List<Long> answers = new LinkedList<>();
    int requestsFromUpstream = 0;
    int requestsToDownstream = 0;

    public ResponseProducer(boolean hasDownstream) {
        this.traversalProducers = new ArrayList<>();
        this.downstreamDone = !hasDownstream;
    }

    public void addTraversalProducer(Iterator<Long> traversalProducer) {
        traversalProducers.add(traversalProducer);
    }

    @Nullable
    public Iterator<Long> getOneTraversalProducer() {
        if (!traversalProducers.isEmpty()) return traversalProducers.get(0);
        return null;
    }

    public void removeTraversalProducer(Iterator<Long> traversalProducer) {
        traversalProducers.remove(traversalProducer);
    }
    public boolean finished() {
        return downstreamDone == true && traversalProducers.isEmpty();
    }

    public void setDownstreamDone() {
        downstreamDone = true;
    }

    public boolean isDownstreamDone() {
        return downstreamDone;
    }

}





