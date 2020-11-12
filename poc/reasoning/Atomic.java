package grakn.common.poc.reasoning;

import grakn.common.collection.Either;
import grakn.common.concurrent.actor.Actor;
import grakn.common.poc.reasoning.execution.ExecutionActor;
import grakn.common.poc.reasoning.execution.Plan;
import grakn.common.poc.reasoning.execution.Request;
import grakn.common.poc.reasoning.execution.Response;
import grakn.common.poc.reasoning.execution.ResponseProducer;
import grakn.common.poc.reasoning.mock.MockTransaction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class Atomic extends ExecutionActor<Atomic> {

    private final Long traversalPattern;
    private final long traversalSize;
    private final List<List<Long>> rules;
    private List<Actor<Rule>> ruleActors;
    private Set<RuleTrigger> triggered;

    public Atomic(final Actor<Atomic> self, final Long traversalPattern, final long traversalSize, final List<List<Long>> rules) {
        super(self, Atomic.class.getSimpleName() + "(pattern: " + traversalPattern + ")");
        this.traversalPattern = traversalPattern;
        this.traversalSize = traversalSize;
        this.rules = rules;
        ruleActors = new ArrayList<>();
        triggered = new HashSet<>();
    }

    @Override
    public Either<Request, Response> receiveRequest(final Request fromUpstream, final ResponseProducer responseProducer) {
        Plan responsePlan = respondingPlan(fromUpstream);

        if (responseProducer.getOneTraversalProducer() != null) {
            List<Long> answers = produceTraversalAnswer(responseProducer);
            return Either.second(
                    new Response.Answer(fromUpstream, responsePlan, answers, fromUpstream.constraints(), fromUpstream.unifiers()));
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
        if (answerSource(fromDownstream).state instanceof Atomic) {
            registerTraversal(responseProducer, fromDownstream.partialAnswer());
            RuleTrigger trigger = new RuleTrigger(fromDownstream.partialAnswer(), fromDownstream.constraints());
            if (!triggered.contains(trigger)) {
                registerDownstreamRules(responseProducer, fromDownstream.plan(), fromDownstream.partialAnswer(),
                        fromDownstream.constraints(), fromDownstream.unifiers());
                triggered.add(trigger);
            }

            if (responseProducer.getOneTraversalProducer() != null) {
                List<Long> answers = produceTraversalAnswer(responseProducer);
                return Either.second(
                        new Response.Answer(fromUpstream, forwardingPlan, answers, fromUpstream.constraints(), fromUpstream.unifiers()));
            } else if (!responseProducer.downstreamsExhausted()) {
                return Either.first(responseProducer.getAvailableDownstream());
            } else {
                return Either.second(new Response.Exhausted(fromUpstream, forwardingPlan));
            }
        } else if (answerSource(fromDownstream).state instanceof Rule) {
            // TODO may combine with partial answers from the fromUpstream message
            return Either.second(
                    new Response.Answer(fromUpstream, forwardingPlan, fromDownstream.partialAnswer(),
                            fromUpstream.constraints(), fromUpstream.unifiers()));
        } else {
            throw new RuntimeException("Unhandled downstream actor of type " +
                    answerSource(fromDownstream).state.getClass().getSimpleName());
        }
    }

    @Override
    public Either<Request, Response> receiveExhausted(final Request fromUpstream, final Response.Exhausted fromDownstream, final ResponseProducer responseProducer) {
        responseProducer.downstreamExhausted(fromDownstream.sourceRequest());
        Plan responsePlan = respondingPlan(fromUpstream);
        if (responseProducer.getOneTraversalProducer() != null) {
            List<Long> answer = produceTraversalAnswer(responseProducer);
            return Either.second(
                    new Response.Answer(fromUpstream, responsePlan, answer, fromUpstream.constraints(), fromUpstream.unifiers()));
        } else if (!responseProducer.downstreamsExhausted()) {
            return Either.first(responseProducer.getAvailableDownstream());
        } else {
            return Either.second(new Response.Exhausted(fromUpstream, responsePlan));
        }
    }

    @Override
    protected ResponseProducer createResponseProducer(final Request request) {

        ResponseProducer responseProducer = new ResponseProducer();

        boolean hasDownstream = request.plan().nextStep() != null;
        if (hasDownstream) {
            Plan nextStep = request.plan().toNextStep();
            Request toDownstream = new Request(nextStep, request.partialAnswer(), request.constraints(), request.unifiers());
            responseProducer.addAvailableDownstream(toDownstream);
        } else {
            registerTraversal(responseProducer, request.partialAnswer());
            RuleTrigger trigger = new RuleTrigger(request.partialAnswer(), request.constraints());
            if (!triggered.contains(trigger)) {
                registerDownstreamRules(responseProducer, request.plan(), request.partialAnswer(),
                        request.constraints(), request.unifiers());
                triggered.add(trigger);
            }

        }
        return responseProducer;
    }

    @Override
    protected void initialiseDownstreamActors(Registry registry) {
        for (List<Long> rule : rules) {
            Actor<Rule> ruleActor = registry.registerRule(rule, pattern ->
                    child(actor -> new Rule(actor, pattern, 1L)));
            ruleActors.add(ruleActor);
        }
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
        for (Actor<Rule> ruleActor : ruleActors) {
            Plan toRule = basePlan.addStep(ruleActor).toNextStep();
            Request toDownstream = new Request(toRule, partialAnswers, constraints, unifiers);
            responseProducer.addAvailableDownstream(toDownstream);
        }
    }

    private Actor<? extends ExecutionActor<?>> answerSource(final Response.Answer answer) {
        return answer.sourceRequest().plan().currentStep();
    }

    private Plan respondingPlan(final Request fromUpstream) {
        return fromUpstream.plan().truncate().endStepCompleted();
    }

    private Plan forwardingPlan(final Response.Answer fromDownstream) {
        return fromDownstream.plan().endStepCompleted();
    }


    private static class RuleTrigger {
        private final List<Long> partialAnswer;
        private final List<Object> constraints;

        public RuleTrigger(List<Long> partialAnswer, List<Object> constraints) {
            this.partialAnswer = partialAnswer;
            this.constraints = constraints;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RuleTrigger that = (RuleTrigger) o;
            return Objects.equals(partialAnswer, that.partialAnswer) &&
                    Objects.equals(constraints, that.constraints);
        }

        @Override
        public int hashCode() {
            return Objects.hash(partialAnswer, constraints);
        }
    }

}

