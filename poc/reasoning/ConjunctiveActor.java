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

import static grakn.common.collection.Collections.list;

public class ConjunctiveActor extends ReasoningActor<ConjunctiveActor> {
    private final Logger LOG;

    private final String name;
    private final List<Long> conjunction;
    private final List<Actor<AtomicActor>> plannedAtomics;
    private final Long traversalSize;
    @Nullable
    private final LinkedBlockingQueue<Long> responses;
    private final Map<Request, ResponseProducer> responseProducers;
    private final Map<Request, Request> requestRouter;


    protected ConjunctiveActor(final Actor<ConjunctiveActor> self, final ActorRegistry actorRegistry, final List<Long> conjunction,
                               final Long traversalSize, final LinkedBlockingQueue<Long> responses) {
        super(self, actorRegistry);
        LOG = LoggerFactory.getLogger(ConjunctiveActor.class.getSimpleName() + "-" + conjunction);

        this.name = "ConjunctiveActor(pattern:" + conjunction + ")";
        this.conjunction = conjunction;
        this.traversalSize = traversalSize;
        this.responses = responses;
        this.plannedAtomics = plan(actorRegistry, this.conjunction);
        responseProducers = new HashMap<>();
        requestRouter = new HashMap<>();
    }

    @Override
    public void receiveRequest(final Request fromUpstream) {
        LOG.debug("Received request in: " + name);
        assert fromUpstream.plan.atEnd() : "A conjunction that receives a request must be at the end of the plan";

        initialiseResponseProducer(fromUpstream);

        Plan responsePlan = getResponsePlan(fromUpstream);

        if (noMoreAnswersPossible(fromUpstream)) respondDoneToUpstream(fromUpstream, responsePlan);
        else {
            // TODO if we want batching, we increment by as many as are requested
            incrementRequestsFromUpstream(fromUpstream);

            if (upstreamHasRequestsOutstanding(fromUpstream)) {
                traverseAndRespond(fromUpstream, responsePlan);
            }

            if (upstreamHasRequestsOutstanding(fromUpstream) && downstreamAvailable(fromUpstream)) {
                requestFromAvailableDownstream(fromUpstream);
            }
        }
    }

    @Override
    public void receiveAnswer(final Response.Answer fromDownstream) {
        LOG.debug("Received answer response in: " + name);
        Request sentDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = requestRouter.get(sentDownstream);

        decrementRequestToDownstream(fromUpstream);

        // directly pass answer response back after combining into a single answer
        Long mergedAnswers = computeAnswer(fromDownstream.partialAnswers);
        bufferAnswer(fromUpstream, mergedAnswers);

        Plan forwardingPlan = forwardingPlan(fromDownstream);
        respondAnswersToUpstream(
                fromUpstream,
                forwardingPlan,
                fromUpstream.partialAnswers,
                fromUpstream.constraints,
                fromUpstream.unifiers,
                responseProducers.get(fromUpstream),
                forwardingPlan.currentStep()
        );
    }

    @Override
    public void receiveDone(final Response.Done fromDownstream) {
        LOG.debug("Received done response in: " + name);
        Request sentDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = requestRouter.get(sentDownstream);
        decrementRequestToDownstream(fromUpstream);

        // every conjunction has exactly 1 downstream, so a done message must indicate the downstream is done
        downstreamDone(fromUpstream, sentDownstream);
        Plan responsePlan = getResponsePlan(fromUpstream);

        if (noMoreAnswersPossible(fromUpstream))  respondDoneToUpstream(fromUpstream, responsePlan);
        else traverseAndRespond(fromUpstream, responsePlan);
    }

    @Override
    void requestFromAvailableDownstream(final Request fromUpstream) {
        ResponseProducer responseProducer = responseProducers.get(fromUpstream);
        Request toDownstream = responseProducer.getAvailableDownstream();
        Actor<? extends ReasoningActor<?>> downstream = toDownstream.plan.currentStep();

        // TODO we may overwrite if multiple identical requests are sent, when to clean up?
        requestRouter.put(toDownstream, fromUpstream);

        LOG.debug("Requesting from downstream in: " + name);
        downstream.tell(actor -> actor.receiveRequest(toDownstream));
        responseProducers.get(fromUpstream).requestsToDownstream++;
    }

    @Override
    void respondAnswersToUpstream(final Request request,
                                  final Plan plan,
                                  final List<Long> partialAnswers,
                                  final List<Object> constraints,
                                  final List<Object> unifiers,
                                  final ResponseProducer responseProducer,
                                  @Nullable final Actor<? extends ReasoningActor<?>> upstream) {
        // send as many answers as possible to upstream
        for (int i = 0; i < Math.min(responseProducer.requestsFromUpstream, responseProducer.answers.size()); i++) {
            Long answer = responseProducer.answers.remove(0);
            if (upstream == null) {
                // base case - how to return from Actor model
                assert responses != null : this + ": can't return answers because the user answers queue is null";
                LOG.debug("Saving answer to output queue in: " + name);
                responses.add(answer);
            } else {
                List<Long> newAnswers = list(partialAnswers, answer);
                Response.Answer responseAnswer = new Response.Answer(
                        request,
                        plan,
                        newAnswers,
                        constraints,
                        unifiers
                );

                LOG.debug("Responding answer to upstream from actor: " + name);
                upstream.tell((actor) -> actor.receiveAnswer(responseAnswer));
            }
            responseProducer.requestsFromUpstream--;
        }
    }

    @Override
    void respondDoneToUpstream(final Request request, final Plan responsePlan) {
        if (responsePlan.currentStep() == null) {
            // base case - how to return from Actor model
            assert responses != null : this + ": can't return answers because the user answers queue is null";
            LOG.debug("Writing Done to output queue in: " + name);
            responses.add(-1L);
        } else {
            Actor<? extends ReasoningActor<?>> upstream = responsePlan.currentStep();
            Response.Done responseDone = new Response.Done(request, responsePlan);
            LOG.debug("Responding Done to upstream in: " + name);
            upstream.tell((actor) -> actor.receiveDone(responseDone));
        }
    }

    private List<Actor<AtomicActor>> plan(final ActorRegistry actorRegistry, final List<Long> conjunction) {
        List<Long> planned = new ArrayList<>(conjunction);
        Collections.reverse(planned);
        planned = Collections.unmodifiableList(planned);
        List<Actor<AtomicActor>> planAsActors = new ArrayList<>();
        // in the future, we'll check if the atom is rule resolvable first
        for (Long atomicPattern : planned) {
            Actor<AtomicActor> atomicActor = actorRegistry.registerAtomic(atomicPattern, (pattern) ->
                    child((newActor) -> new AtomicActor(newActor, actorRegistry, pattern, 5L, Arrays.asList())));
            planAsActors.add(atomicActor);
        }

        // plan the atomics in the conjunction
        return planAsActors;
    }

    private void initialiseResponseProducer(final Request request) {
        if (!responseProducers.containsKey(request)) {
            ResponseProducer responseProducer = new ResponseProducer();
            responseProducers.put(request, responseProducer);

            Plan nextPlan = request.plan.addSteps(this.plannedAtomics).toNextStep();
            Request toDownstream = new Request(
                    nextPlan,
                    request.partialAnswers,
                    request.constraints,
                    request.unifiers
            );
            responseProducer.addAvailableDownstream(toDownstream);

            Long startingAnswer = conjunction.stream().reduce((acc, val) -> acc + val).get();
            Iterator<Long> traversal = (new MockTransaction(traversalSize, 1)).query(startingAnswer);
            if (traversal.hasNext()) responseProducer.addTraversalProducer(traversal);
        }
    }

    private void traverseAndRespond(final Request fromUpstream, final Plan responsePlan) {
        ResponseProducer responseProducer = responseProducers.get(fromUpstream);
        List<Long> answers = produceTraversalAnswers(responseProducer);
        responseProducer.answers.addAll(answers);
        respondAnswersToUpstream(
                fromUpstream,
                responsePlan,
                fromUpstream.partialAnswers,
                fromUpstream.constraints,
                fromUpstream.unifiers,
                responseProducer,
                responsePlan.currentStep()
        );
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

    private void bufferAnswer(final Request fromUpstream, final Long mergedAnswers) {
        responseProducers.get(fromUpstream).answers.add(mergedAnswers);
    }

    private boolean upstreamHasRequestsOutstanding(final Request fromUpstream) {
        ResponseProducer responseProducer = responseProducers.get(fromUpstream);
        return responseProducer.requestsFromUpstream > responseProducer.requestsToDownstream + responseProducer.answers.size();
    }

    private boolean noMoreAnswersPossible(final Request fromUpstream) {
        return responseProducers.get(fromUpstream).finished();
    }

    private void incrementRequestsFromUpstream(final Request fromUpstream) {
        responseProducers.get(fromUpstream).requestsFromUpstream++;
    }

    private void decrementRequestToDownstream(final Request fromUpstream) {
        responseProducers.get(fromUpstream).requestsToDownstream--;
    }

    private Plan getResponsePlan(final Request fromUpstream) {
        return fromUpstream.plan.endStepCompleted();
    }

    private Plan forwardingPlan(final Response.Answer answer) {
        return answer.plan.endStepCompleted();
    }

    private boolean downstreamAvailable(final Request fromUpstream) {
        return !responseProducers.get(fromUpstream).isDownstreamDone();
    }

    private void downstreamDone(final Request fromUpstream, final Request sentDownstream) {
        responseProducers.get(fromUpstream).downstreamDone(sentDownstream);
    }

    private Long computeAnswer(final List<Long> partialAnswers) {
        return partialAnswers.stream().reduce(0L, (acc, v) -> acc + v);
    }
}
