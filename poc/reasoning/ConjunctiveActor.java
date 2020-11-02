package grakn.common.poc.reasoning;

import grakn.common.concurrent.actor.Actor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class ConjunctiveActor extends ReasoningActor<ConjunctiveActor> {
    Logger LOG;

    private final String name;
    private final List<Long> conjunction;
    final Path path; // TODO open for tests
    private final Long traversalSize;
    @Nullable
    private final LinkedBlockingQueue<Long> responses;
    private final Map<Request, ResponseProducer> requestProducers;
    private final Map<Request, Request> requestRouter;


    protected ConjunctiveActor(final Actor<ConjunctiveActor> self, ActorRegistry actorRegistry, List<Long> conjunction,
                               Long traversalSize, LinkedBlockingQueue<Long> responses) {
        super(self, actorRegistry);
        LOG = LoggerFactory.getLogger(ConjunctiveActor.class.getSimpleName() + "-" + conjunction);

        this.name = "ConjunctiveActor(pattern:" + conjunction + ")";
        this.conjunction = conjunction;
        this.traversalSize = traversalSize;
        this.responses = responses;
        this.path = plan(actorRegistry, this.conjunction);
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

    @Override
    public void receiveAnswer(final Response.Answer answer) {
        LOG.debug("Received answer response in: " + name);
        Request request = answer.request();
        Request parentRequest = requestRouter.get(request);
        ResponseProducer responseProducer = requestProducers.get(parentRequest);
        responseProducer.requestsToDownstream--;

        // directly pass answer response back after combining into a single answer
        List<Long> partialAnswers = answer.partialAnswers;
        Long mergedAnswers = partialAnswers.stream().reduce(0L, (acc, v) -> acc + v);
        responseProducer.answers.add(mergedAnswers);
        respondAnswersToRequester(parentRequest, responseProducer);
    }

    @Override
    public void receiveDone(final Response.Done done) {
        LOG.debug("Received done response in: " + name);
        Request request = done.request();
        Request parentRequest = requestRouter.get(request);
        ResponseProducer responseProducer = requestProducers.get(parentRequest);
        responseProducer.requestsToDownstream--;

        // every conjunction has exactly 1 downstream, so a done message must indicate the downstream is done
        responseProducer.setDownstreamDone();

        if (responseProducer.finished()) {
            respondDoneToRequester(parentRequest);
        } else {
            List<Long> answers = produceTraversalAnswers(responseProducer);
            responseProducer.answers.addAll(answers);
            respondAnswersToRequester(parentRequest, responseProducer);
        }
    }

    @Override
    void requestFromDownstream(final Request request) {
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

        LOG.debug("Requesting from downstream in: " + name);
        downstream.tell(actor -> actor.receiveRequest(subrequest));
        requestProducers.get(request).requestsToDownstream++;
    }

    @Override
    void respondAnswersToRequester(final Request request, final ResponseProducer responseProducer) {
        // send as many answers as possible to requester
        for (int i = 0; i < Math.min(responseProducer.requestsFromUpstream, responseProducer.answers.size()); i++) {
            Long answer = responseProducer.answers.remove(0);
            if (request.path.atRoot()) {
                // base case - how to return from Actor model
                assert responses != null : this + ": can't return answers because the user answers queue is null";
                LOG.debug("Saving answer to output queue in: " + name);
                responses.add(answer);
            } else {
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
            }
            responseProducer.requestsFromUpstream--;
        }
    }

    @Override
    void respondDoneToRequester(final Request request) {
        if (request.path.atRoot()) {
            // base case - how to return from Actor model
            assert responses != null : this + ": can't return answers because the user answers queue is null";
            LOG.debug("Writing Done to output queue in: " + name);
            responses.add(-1L);
        } else {
            Actor<? extends ReasoningActor<?>> requester = request.path.directUpstream();
            Path newPath = request.path.moveUpstream();
            Response.Done responseDone = new Response.Done(request, newPath);
            LOG.debug("Responding Done to requester in: " + name);
            requester.tell((actor) -> actor.receiveDone(responseDone));
        }
    }

    private List<Long> produceTraversalAnswers(final ResponseProducer responseProducer) {
        Iterator<Long> traversalProducer = responseProducer.getOneTraversalProducer();
        if (traversalProducer != null) {
            // TODO could do batch traverse, or retrieve answers from multiple traversals
            Long answer = traversalProducer.next();
            if (!traversalProducer.hasNext()) responseProducer.removeTraversalProducer(traversalProducer);
            return Arrays.asList(answer);
        }
        return Arrays.asList();
    }

    /*
    Given a conjunction, return an ordered list of constraints to traverse
    The first constraint should be the starting point that finds initial answers
    before propagating them in the order indicated by the plan
     */
    private Path plan(ActorRegistry actorRegistry, List<Long> conjunction) {
        List<Long> planned = new ArrayList<>(conjunction);
        Collections.reverse(planned);
        List<Actor<? extends ReasoningActor<?>>> planAsActors = new ArrayList<>();
        planAsActors.add(self());
        // in the future, we'll check if the atom is rule resolvable first
        for (Long atomicPattern : planned) {
            Actor<AtomicActor> atomicActor = actorRegistry.registerAtomic(atomicPattern, (pattern) ->
                    child((newActor) -> new AtomicActor(newActor, actorRegistry, pattern, 5L, Arrays.asList())));
            planAsActors.add(atomicActor);
        }

        // plan the atomics in the conjunction
        return new Path(planAsActors);
    }

    private ResponseProducer initialiseResponseProducer(final Request request) {
        boolean downstreamExists = request.path.directDownstream() != null;
        ResponseProducer responseProducer = new ResponseProducer(downstreamExists);
        Long startingAnswer = conjunction.stream().reduce((acc, val) -> acc + val).get();
        Iterator<Long> traversal = (new MockTransaction(traversalSize, 1)).query(startingAnswer);
        if (traversal.hasNext()) responseProducer.addTraversalProducer(traversal);
        return responseProducer;
    }
}
