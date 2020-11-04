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

    private final String name;
    private final Long traversalPattern;
    private final long traversalSize;
    private final Map<Request, ResponseProducer> responseProducers;
    // TODO EH???? what is the below comment
    // TODO note that this can be many to one, and is not catered for yet (ie. request followed the same request)
    private final Map<Request, Request> requestRouter;
    private final List<Actor<RuleActor>> ruleActors;

    public AtomicActor(final Actor<AtomicActor> self, ActorRegistry actorRegistry, Long traversalPattern, final long traversalSize, List<List<Long>> rules) {
        super(self, actorRegistry);
        LOG = LoggerFactory.getLogger(AtomicActor.class.getSimpleName() + "-" + traversalPattern);

        this.name = "AtomicActor(pattern: " + traversalPattern + ")";
        this.traversalPattern = traversalPattern;
        this.traversalSize = traversalSize;
        responseProducers = new HashMap<>();
        requestRouter = new HashMap<>();
        ruleActors = registerRuleActors(actorRegistry, rules);
    }

    private List<Actor<RuleActor>> registerRuleActors(final ActorRegistry actorRegistry, final List<List<Long>> rules) {
        final List<Actor<RuleActor>> ruleActors = new ArrayList<>();
        for (List<Long> rule : rules) {
            Actor<RuleActor> ruleActor = actorRegistry.registerRule(rule, pattern ->
                    child(actor -> new RuleActor(actor, actorRegistry, pattern, 1L))
            );
            ruleActors.add(ruleActor);
        }
        return ruleActors;
    }

    @Override
    public void receiveRequest(final Request fromUpstream) {
        LOG.debug("Received fromUpstream in: " + name);

        responseProducers.computeIfAbsent(fromUpstream, key -> initialiseResponseProducer(fromUpstream));

        Plan responsePlan = getResponsePlan(fromUpstream);

        if (noMoreAnswersPossible(fromUpstream)) respondDoneToUpstream(fromUpstream, responsePlan);
        else {
            // TODO if we want batching, we increment by as many as are requested
            incrementRequestsFromUpstream(fromUpstream);

            if (upstreamHasRequestsOutstanding(fromUpstream)) {
                traverseAndRespond(fromUpstream, responsePlan);
            }

            if (upstreamHasRequestsOutstanding(fromUpstream) && downstreamAvailable(fromUpstream)) {
                requestFromDownstream(fromUpstream);
            }
        }
    }

    private boolean downstreamAvailable(Request fromUpstream) {
        return !responseProducers.get(fromUpstream).isDownstreamDone();
    }

    private void traverseAndRespond(Request fromUpstream, Plan responsePlan) {
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

    private boolean upstreamHasRequestsOutstanding(Request fromUpstream) {
        ResponseProducer responseProducer = responseProducers.get(fromUpstream);
        return responseProducer.requestsFromUpstream > responseProducer.requestsToDownstream + responseProducer.answers.size();
    }

    private void incrementRequestsFromUpstream(Request fromUpstream) {
        responseProducers.get(fromUpstream).requestsFromUpstream++;
    }

    private boolean noMoreAnswersPossible(Request fromUpstream) {
        return responseProducers.get(fromUpstream).finished();
    }

    private Plan getResponsePlan(Request fromUpstream) {
        return fromUpstream.plan.truncate().endStepCompleted();
    }

    /*
    When a receive and answer and pass the answer forward
    We map the request that generated the answer, to the originating request.
    We then copy the originating request, and clear the request path, as it must already have been satisfied.
     */
    @Override
    public void receiveAnswer(final Response.Answer answer) {
        LOG.debug("Received answer response in: " + name);
        Request request = answer.sourceRequest();
        Request fromUpstream = requestRouter.get(request);
        ResponseProducer responseProducer = responseProducers.get(fromUpstream);
        responseProducer.requestsToDownstream--;

        List<Long> partialAnswers = answer.partialAnswers;
        Plan responsePlan = answer.plan.endStepCompleted();

        // TODO fix accessing state
        if (request.plan.currentStep().state instanceof AtomicActor) {
            registerTraversal(responseProducer, partialAnswers);
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

            registerRuleDownstreams(
                    answer.plan,
                    answer.partialAnswers,
                    answer.constraints,
                    answer.unifiers,
                    responseProducer
            );
            if (responseProducer.requestsFromUpstream > responseProducer.requestsToDownstream + responseProducer.answers.size()) {
                if (!responseProducer.isDownstreamDone()) {
                    requestFromDownstream(fromUpstream);
                }
            }
        } else if (request.plan.currentStep().state instanceof RuleActor) {
            Long mergedAnswer = partialAnswers.stream().reduce(0L, (acc, v) -> acc + v);
            responseProducer.answers.add(mergedAnswer);
            respondAnswersToUpstream(
                    fromUpstream,
                    responsePlan,
                    Arrays.asList(),
                    fromUpstream.constraints,
                    fromUpstream.unifiers,
                    responseProducer,
                    responsePlan.currentStep()
            );
        } else {
            throw new RuntimeException("Unhandled downstream actor of type " + request.plan.nextStep().state.getClass().getSimpleName());
        }

        if (responseProducer.finished()) {
            respondDoneToUpstream(request, responsePlan);
        }
    }

    private void registerRuleDownstreams(
            Plan basePlan,
            List<Long> partialAnswers,
            List<Object> constraints,
            List<Object> unifiers,
            final ResponseProducer responseProducer) {
        for (Actor<RuleActor> ruleActor : ruleActors) {
            Plan toRule = basePlan.addStep(ruleActor).toNextStep();
            Request toDownstream = new Request(
                    toRule,
                    partialAnswers,
                    constraints,
                    unifiers
            );
            responseProducer.addAvailableDownstream(toDownstream);
        }
    }

    @Override
    public void receiveDone(final Response.Done done) {
        LOG.debug("Received done response in: " + name);
        Request request = done.sourceRequest();
        Request fromUpstream = requestRouter.get(request);
        ResponseProducer responseProducer = responseProducers.get(fromUpstream);
        responseProducer.requestsToDownstream--;

        responseProducer.downstreamDone(request);

        Plan responsePlan = done.plan.endStepCompleted();
        if (responseProducer.finished()) {
            respondDoneToUpstream(fromUpstream, responsePlan);
        } else {
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

            if (responseProducer.requestsFromUpstream > responseProducer.requestsToDownstream + responseProducer.answers.size()) {
                if (!responseProducer.isDownstreamDone()) {
                    requestFromDownstream(fromUpstream);
                }
            }
        }
    }

    @Override
    void requestFromDownstream(final Request fromUpstream) {
        ResponseProducer responseProducer = responseProducers.get(fromUpstream);
        Request toDownstream = responseProducer.toDownstream();
        Actor<? extends ReasoningActor<?>> downstream = toDownstream.plan.currentStep();
        responseProducer.requestsToDownstream++;
        // TODO we may overwrite if multiple identical requests are sent, when to clean up?
        requestRouter.put(toDownstream, fromUpstream);

        LOG.debug("Requesting from downstream in: " + name);
        downstream.tell(actor -> actor.receiveRequest(toDownstream));
    }

    @Override
    void respondAnswersToUpstream(
            final Request request,
            final Plan plan,
            final List<Long> partialAnswers,
            final List<Object> constraints,
            final List<Object> unifiers,
            final ResponseProducer responseProducer,
            final Actor<? extends ReasoningActor<?>> upstream
    ) {
        // send as many answers as possible to upstream
        for (int i = 0; i < Math.min(responseProducer.requestsFromUpstream, responseProducer.answers.size()); i++) {
            Long answer = responseProducer.answers.remove(0);
            List<Long> newAnswers = new ArrayList<>(partialAnswers);
            newAnswers.add(answer);
            Response.Answer responseAnswer = new Response.Answer(
                    request,
                    plan,
                    newAnswers,
                    constraints,
                    unifiers
            );

            LOG.debug("Responding answer to upstream from actor: " + name);
            upstream.tell((actor) -> actor.receiveAnswer(responseAnswer));
            responseProducer.requestsFromUpstream--;
        }
    }

    @Override
    void respondDoneToUpstream(final Request request, final Plan responsePlan) {
        Actor<? extends ReasoningActor<?>> upstream = responsePlan.currentStep();
        Response.Done responseDone = new Response.Done(request, responsePlan);
        LOG.debug("Responding Done to upstream from actor: " + name);
        upstream.tell((actor) -> actor.receiveDone(responseDone));
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
        ResponseProducer responseProducer = new ResponseProducer();

        boolean hasDownstream = request.plan.nextStep() != null;
        if (hasDownstream) {
            Plan nextStep = request.plan.toNextStep();
            Request toDownstream = new Request(
                    nextStep,
                    request.partialAnswers,
                    request.constraints,
                    request.unifiers
            );
            responseProducer.addAvailableDownstream(toDownstream);
        } else {
            registerTraversal(responseProducer, request.partialAnswers);
            registerRuleDownstreams(
                    request.plan,
                    request.partialAnswers,
                    request.constraints,
                    request.unifiers,
                    responseProducer
            );
        }

        return responseProducer;
    }

    private void registerTraversal(final ResponseProducer responseProducer, List<Long> partialAnswers) {
        Long mergedAnswers = partialAnswers.stream().reduce(0L, (acc, v) -> acc + v);
        Iterator<Long> traversal = (new MockTransaction(traversalSize, 1)).query(mergedAnswers);
        if (traversal.hasNext()) responseProducer.addTraversalProducer(traversal);
    }
}

