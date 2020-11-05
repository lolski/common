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
    private final List<Actor<RuleActor>> ruleActors;

    public AtomicActor(final Actor<AtomicActor> self, final ActorRegistry actorRegistry, final Long traversalPattern, final long traversalSize, final List<List<Long>> rules) {
        super(self, actorRegistry, AtomicActor.class.getSimpleName() + "(pattern: " + traversalPattern + ")");
        this.traversalPattern = traversalPattern;
        this.traversalSize = traversalSize;
        ruleActors = registerRuleActors(actorRegistry, rules);
    }

    private List<Actor<RuleActor>> registerRuleActors(final ActorRegistry actorRegistry, final List<List<Long>> rules) {
        final List<Actor<RuleActor>> ruleActors = new ArrayList<>();
        for (List<Long> rule : rules) {
            System.out.println("rule: " + rule.get(0));
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

        if (responseProducer.getOneTraversalProducer() != null) {
            List<Long> answers = produceTraversalAnswer(responseProducer);
            return Either.second(
                    new Response.Answer(fromUpstream, responsePlan, answers, fromUpstream.constraints, fromUpstream.unifiers));
        } else if (!responseProducer.downstreamsExhausted()) {
            return Either.first(responseProducer.getAvailableDownstream());
        } else {
            return Either.second(new Response.Exhausted(fromUpstream, responsePlan));
        }
    }

    @Override
    public Either<Request, Response> receiveAnswer(final Request fromUpstream, final Response.Answer fromDownstream, ResponseProducer responseProducer) {
        Plan forwardingPlan = forwardingPlan(fromDownstream);

        // TODO fix accessing actor state directly
        if (answerSource(fromDownstream).state instanceof AtomicActor) {
            registerTraversal(responseProducer, fromDownstream.partialAnswers);
            registerDownstreamRules(responseProducer, fromDownstream.plan, fromDownstream.partialAnswers,
                    fromDownstream.constraints, fromDownstream.unifiers);

            if (responseProducer.getOneTraversalProducer() != null) {
                List<Long> answers = produceTraversalAnswer(responseProducer);
                return Either.second(
                        new Response.Answer(fromUpstream, forwardingPlan, answers, fromUpstream.constraints, fromUpstream.unifiers));
            } else if (!responseProducer.downstreamsExhausted()) {
                return Either.first(responseProducer.getAvailableDownstream());
            } else {
                return Either.second(new Response.Exhausted(fromUpstream, forwardingPlan));
            }
        } else if (answerSource(fromDownstream).state instanceof RuleActor) {
            // TODO may combine with partial answers from the fromUpstream message
            return Either.second(
                    new Response.Answer(fromUpstream, forwardingPlan, fromDownstream.partialAnswers,
                            fromUpstream.constraints, fromUpstream.unifiers));
        } else {
            throw new RuntimeException("Unhandled downstream actor of type " +
                    answerSource(fromDownstream).state.getClass().getSimpleName());
        }
    }

    @Override
    public Either<Request, Response> receiveExhausted(final Request fromUpstream, final Response.Exhausted fromDownstream, final ResponseProducer responseProducer) {
        responseProducer.downstreamExhausted(fromDownstream.sourceRequest());
        Plan responsePlan = getResponsePlan(fromUpstream);
        if (responseProducer.getOneTraversalProducer() != null) {
            List<Long> answer = produceTraversalAnswer(responseProducer);
            return Either.second(
                    new Response.Answer(fromUpstream, responsePlan, answer, fromUpstream.constraints, fromUpstream.unifiers));
        } else if (!responseProducer.downstreamsExhausted()) {
            return Either.first(responseProducer.getAvailableDownstream());
        } else {
            return Either.second(new Response.Exhausted(fromUpstream, responsePlan));
        }
    }

    @Override
    ResponseProducer createResponseProducer(final Request request) {
        ResponseProducer responseProducer = new ResponseProducer();

        boolean hasDownstream = request.plan().nextStep() != null;
        if (hasDownstream) {
            Plan nextStep = request.plan().toNextStep();
            Request toDownstream = new Request(nextStep, request.partialAnswers, request.constraints, request.unifiers);
            responseProducer.addAvailableDownstream(toDownstream);
        } else {
            registerTraversal(responseProducer, request.partialAnswers);
            registerDownstreamRules(responseProducer, request.plan(), request.partialAnswers, request.constraints, request.unifiers);
        }
        return responseProducer;
    }

    private List<Long> produceTraversalAnswer(final ResponseProducer responseProducer) {
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

    private void registerDownstreamRules(final ResponseProducer responseProducer, final Plan basePlan, final List<Long> partialAnswers,
                                         final List<Object> constraints, final List<Object> unifiers) {
        for (Actor<RuleActor> ruleActor : ruleActors) {
            Plan toRule = basePlan.addStep(ruleActor).toNextStep();
            Request toDownstream = new Request(toRule, partialAnswers, constraints, unifiers);
            responseProducer.addAvailableDownstream(toDownstream);
        }
    }

    private Actor<? extends ExecutionActor<?>> answerSource(final Response.Answer answer) {
        return answer.sourceRequest().plan().currentStep();
    }

    private Plan getResponsePlan(final Request fromUpstream) {
        return fromUpstream.plan().truncate().endStepCompleted();
    }

    private Plan forwardingPlan(final Response.Answer fromDownstream) {
        return fromDownstream.plan().endStepCompleted();
    }
}

