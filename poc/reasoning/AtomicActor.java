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
    private final Map<Request, ResponseProducer> requestProducers;
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
        requestProducers = new HashMap<>();
        requestRouter = new HashMap<>();
        ruleActors = registerRuleActors(actorRegistry, rules);
    }

    private List<Actor<RuleActor>> registerRuleActors(final ActorRegistry actorRegistry, final List<List<Long>> rules) {
        final List<Actor<RuleActor>> ruleActors = new ArrayList<>();
        for (List<Long> rule : rules) {
            actorRegistry.registerRule(rule, pattern ->
                    child(actor -> new RuleActor(actor, actorRegistry, pattern, 1L))
            );
        }
        return ruleActors;
    }

    @Override
    public void receiveRequest(final Request request) {
        LOG.debug("Received request in: " + name);
        if (!this.requestProducers.containsKey(request)) {
            this.requestProducers.put(request, initialiseResponseProducer(request));
        }

        ResponseProducer responseProducer = this.requestProducers.get(request);

        Plan responsePlan = request.plan.truncate().endStepCompleted();
        if (responseProducer.finished()) {
            respondDoneToUpstream(request, responsePlan);
        } else {
            // TODO if we want batching, we increment by as many as are requested
            responseProducer.requestsFromUpstream++;

            if (responseProducer.requestsFromUpstream > responseProducer.requestsToDownstream + responseProducer.answers.size()) {
                List<Long> answers = produceTraversalAnswers(responseProducer);
                responseProducer.answers.addAll(answers);
                respondAnswersToUpstream(
                        request,
                        responsePlan,
                        request.partialAnswers,
                        request.constraints,
                        request.unifiers,
                        responseProducer,
                        responsePlan.currentStep()
                );
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
        responseProducer.requestsToDownstream--;

        List<Long> partialAnswers = answer.partialAnswers;
        Long mergedAnswers = partialAnswers.stream().reduce(0L, (acc, v) -> acc + v);
        registerTraversal(responseProducer, mergedAnswers);

        List<Long> answers = produceTraversalAnswers(responseProducer);
        responseProducer.answers.addAll(answers);

        Plan shortenedPlan = answer.plan.endStepCompleted();

        respondAnswersToUpstream(
                parentRequest,
                shortenedPlan,
                parentRequest.partialAnswers,
                parentRequest.constraints,
                parentRequest.unifiers,
                responseProducer,
                shortenedPlan.currentStep()
        );
    }

    @Override
    public void receiveDone(final Response.Done done) {
        LOG.debug("Received done response in: " + name);
        Request request = done.request();
        Request parentRequest = requestRouter.get(request);
        ResponseProducer responseProducer = requestProducers.get(parentRequest);
        responseProducer.requestsToDownstream--;

        responseProducer.setDownstreamDone();

        Plan responsePlan = done.plan.endStepCompleted();
        if (responseProducer.finished()) {
            respondDoneToUpstream(parentRequest, responsePlan);
        } else {
            List<Long> answers = produceTraversalAnswers(responseProducer);
            responseProducer.answers.addAll(answers);
            respondAnswersToUpstream(
                    parentRequest,
                    responsePlan,
                    parentRequest.partialAnswers,
                    parentRequest.constraints,
                    parentRequest.unifiers,
                    responseProducer,
                    responsePlan.currentStep()
            );
        }
    }

    @Override
    void requestFromDownstream(final Request request) {
        // TODO open question - should downstream requests increment "requested" field?
        Actor<? extends ReasoningActor<?>> downstream = request.plan.nextStep();
        Plan nextStep = request.plan.toNextStep();
        Request subrequest = new Request(
                nextStep,
                request.partialAnswers,
                request.constraints,
                request.unifiers
        );

        // TODO we may overwrite if multiple identical requests are sent, when to clean up?
        requestRouter.put(subrequest, request);

        LOG.debug("Requesting from downstream in: " + name);
        downstream.tell(actor -> actor.receiveRequest(subrequest));
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
        // send as many answers as possible to requester
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
        Actor<? extends ReasoningActor<?>> requester = responsePlan.currentStep();
        Response.Done responseDone = new Response.Done(request, responsePlan);
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
        boolean hasNextStep = request.plan.nextStep() != null;
        ResponseProducer responseProducer = new ResponseProducer(hasNextStep);

        if (responseProducer.isDownstreamDone()) {
            registerTraversal(responseProducer, 0L);
        }

        return responseProducer;
    }

    private void registerTraversal(final ResponseProducer responseProducer, long partialAnswer) {
        Iterator<Long> traversal = (new MockTransaction(traversalSize, 1)).query(partialAnswer);
        responseProducer.addTraversalProducer(traversal);
    }
}

