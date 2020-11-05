package grakn.common.poc.reasoning;

import grakn.common.collection.Either;
import grakn.common.concurrent.actor.Actor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class AtomicActor extends ExecutionActor<AtomicActor> {

    private final Long traversalPattern;
    private final long traversalSize;
    // TODO EH???? what is the below comment
    // TODO note that this can be many to one, and is not catered for yet (ie. request followed the same request)
    private final List<Actor<RuleActor>> ruleActors;

    public AtomicActor(final Actor<AtomicActor> self, final ActorRegistry actorRegistry, final Long traversalPattern, final long traversalSize, final List<List<Long>> rules) {
        super(self, actorRegistry, "AtomicActor(pattern: " + traversalPattern + ")");
        this.traversalPattern = traversalPattern;
        this.traversalSize = traversalSize;
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
    public Either<Request, Response> receiveRequest(final Request fromUpstream, final ResponseProducer responseProducer) {
        Plan responsePlan = getResponsePlan(fromUpstream);

        if (noMoreAnswersPossible(fromUpstream)) {
            return Either.second(new Response.Exhausted(fromUpstream, responsePlan));
        }
        else {
            if (responseProducer.getOneTraversalProducer() != null) {
                List<Long> answers = produceTraversalAnswers(responseProducer);
                return Either.second(
                        new Response.Answer(
                                fromUpstream,
                                responsePlan,
                                answers,
                                fromUpstream.constraints,
                                fromUpstream.unifiers
                        )
                );
            } else {
                return Either.first(responseProducer.getAvailableDownstream());
            }
        }
    }

    @Override
    public void receiveAnswer(final Response.Answer fromDownstream) {
        LOG.debug("Received fromDownstream response in: " + name);
        Request sentDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = requestRouter.get(sentDownstream);

        decrementRequestsToDownstream(fromUpstream);

        Plan forwardingPlan = forwardingPlan(fromDownstream);

        // TODO fix accessing state
        if (answerSource(fromDownstream).state instanceof AtomicActor) {
            registerTraversal(responseProducers.get(fromUpstream), fromDownstream.partialAnswers);
            traverseAndRespond(fromUpstream, forwardingPlan);
            registerDownstreamRules(
                    fromUpstream,
                    fromDownstream.plan,
                    fromDownstream.partialAnswers,
                    fromDownstream.constraints,
                    fromDownstream.unifiers
            );
        } else if (answerSource(fromDownstream).state instanceof RuleActor) {
            respondAnswerToUpstream(
                    fromUpstream,
                    forwardingPlan,
                    fromDownstream.partialAnswers, // TODO may combine with partial answers from the fromUpstream message
                    fromUpstream.constraints,
                    fromUpstream.unifiers,
                    responseProducers.get(fromUpstream),
                    forwardingPlan.currentStep()
            );
        } else {
            throw new RuntimeException("Unhandled downstream actor of type " +
                    sentDownstream.plan.nextStep().state.getClass().getSimpleName());
        }

        if (upstreamHasRequestsOutstanding(fromUpstream) && downstreamAvailable(fromUpstream)) {
            requestFromAvailableDownstream(fromUpstream);
        }
    }

    @Override
    public void receiveExhausted(final Response.Exhausted fromDownstream) {
        LOG.debug("Received fromDownstream response in: " + name);
        Request sentDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = requestRouter.get(sentDownstream);
        decrementRequestsToDownstream(fromUpstream);

        downstreamExhausted(fromUpstream, sentDownstream);

        Plan responsePlan = getResponsePlan(fromUpstream);
        if (noMoreAnswersPossible(fromUpstream)) {
            respondExhaustedToUpstream(fromUpstream, responsePlan);
        } else {
            traverseAndRespond(fromUpstream, responsePlan);

            if (upstreamHasRequestsOutstanding(fromUpstream) && downstreamAvailable(fromUpstream)) {
                requestFromAvailableDownstream(fromUpstream);
            }
        }
    }

    @Override
    void requestFromAvailableDownstream(final Request fromUpstream) {
        ResponseProducer responseProducer = responseProducers.get(fromUpstream);
        Request toDownstream = responseProducer.getAvailableDownstream();
        Actor<? extends ExecutionActor<?>> downstream = toDownstream.plan.currentStep();
        responseProducer.incrementRequestsToDownstream();
        // TODO we may overwrite if multiple identical requests are sent, when to clean up?
        requestRouter.put(toDownstream, fromUpstream);

        LOG.debug("Requesting from downstream in: " + name);
        downstream.tell(actor -> actor.receiveRequest(toDownstream));
    }

    @Override
    void respondAnswerToUpstream(
            final Request request,
            final Plan plan,
            final List<Long> partialAnswer,
            final List<Object> constraints,
            final List<Object> unifiers,
            final ResponseProducer responseProducer,
            final Actor<? extends ExecutionActor<?>> upstream
    ) {
        Response.Answer responseAnswer = new Response.Answer(
                request,
                plan,
                partialAnswer,
                constraints,
                unifiers
        );

        LOG.debug("Responding answer to upstream from actor: " + name);
        upstream.tell((actor) -> actor.receiveAnswer(responseAnswer));
        responseProducer.decrementRequestsFromUpstream();
    }


    @Override
    void respondExhaustedToUpstream(final Request request, final Plan responsePlan) {
        Actor<? extends ExecutionActor<?>> upstream = responsePlan.currentStep();
        Response.Exhausted responseExhausted = new Response.Exhausted(request, responsePlan);
        LOG.debug("Responding Exhausted to upstream from actor: " + name);
        upstream.tell((actor) -> actor.receiveExhausted(responseExhausted));
    }

    @Override
    ResponseProducer createResponseProducer(final Request request) {
        ResponseProducer responseProducer = new ResponseProducer();

        boolean hasDownstream = request.plan().nextStep() != null;
        if (hasDownstream) {
            Plan nextStep = request.plan().toNextStep();
            Request toDownstream = new Request(
                    nextStep,
                    request.partialAnswers,
                    request.constraints,
                    request.unifiers
            );
            responseProducer.addAvailableDownstream(toDownstream);
        } else {
            registerTraversal(responseProducer, request.partialAnswers);
            registerDownstreamRules(
                    request,
                    request.plan(),
                    request.partialAnswers,
                    request.constraints,
                    request.unifiers
            );
        }
        return responseProducer;
    }

    private void traverseAndRespond(final Request fromUpstream, final Plan responsePlan) {
        ResponseProducer responseProducer = responseProducers.get(fromUpstream);
        if (responseProducer.getOneTraversalProducer() != null) {
            List<Long> answers = produceTraversalAnswers(responseProducer);
            respondAnswerToUpstream(
                    fromUpstream,
                    responsePlan,
                    answers, // TODO may combined with partial answers in fromUpstream
                    fromUpstream.constraints,
                    fromUpstream.unifiers,
                    responseProducer,
                    responsePlan.currentStep()
            );
        }
    }

    private List<Long> produceTraversalAnswers(final ResponseProducer responseProducer) {
        Iterator<Long> traversalProducer = responseProducer.getOneTraversalProducer();
        // TODO could do batch traverse, or retrieve answers from multiple traversals
        Long answer = traversalProducer.next();
        if (!traversalProducer.hasNext()) responseProducer.removeTraversalProducer(traversalProducer);
        answer += this.traversalPattern;
        return Arrays.asList(answer);
    }

    private void registerTraversal(ResponseProducer responseProducer, final List<Long> partialAnswer) {
        Long mergedAnswer = partialAnswer.stream().reduce(0L, (acc, v) -> acc + v);
        Iterator<Long> traversal = (new MockTransaction(traversalSize, 1)).query(mergedAnswer);
        if (traversal.hasNext()) responseProducer.addTraversalProducer(traversal);
    }

    private void registerDownstreamRules(final Request request, final Plan basePlan, final List<Long> partialAnswers,
                                         final List<Object> constraints, final List<Object> unifiers) {
        for (Actor<RuleActor> ruleActor : ruleActors) {
            Plan toRule = basePlan.addStep(ruleActor).toNextStep();
            Request toDownstream = new Request(toRule, partialAnswers, constraints, unifiers);
            responseProducers.get(request).addAvailableDownstream(toDownstream);
        }
    }

    private boolean upstreamHasRequestsOutstanding(final Request fromUpstream) {
        ResponseProducer responseProducer = responseProducers.get(fromUpstream);
        return responseProducer.requestsFromUpstream() > responseProducer.requestsToDownstream();
    }

    private boolean noMoreAnswersPossible(final Request fromUpstream) {
        return responseProducers.get(fromUpstream).noMoreAnswersPossible();
    }

    private void incrementRequestsFromUpstream(final Request fromUpstream) {
        responseProducers.get(fromUpstream).incrementRequestsFromUpstream();
    }

    private void decrementRequestsToDownstream(final Request fromUpstream) {
        responseProducers.get(fromUpstream).decrementRequestsToDownstream();
    }

    private Actor<? extends ExecutionActor<?>> answerSource(final Response.Answer answer) {
        return answer.sourceRequest().plan.currentStep();
    }

    private Plan getResponsePlan(final Request fromUpstream) {
        return fromUpstream.plan.truncate().endStepCompleted();
    }

    private Plan forwardingPlan(final Response.Answer fromDownstream) {
        return fromDownstream.plan.endStepCompleted();
    }

    private boolean downstreamAvailable(final Request fromUpstream) {
        return !responseProducers.get(fromUpstream).downstreamExhausted();
    }

    private void downstreamExhausted(final Request fromUpstream, final Request sentDownstream) {
        responseProducers.get(fromUpstream).downstreamExhausted(sentDownstream);
    }
}

