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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class Rule extends ExecutionActor<Rule> {
    private final List<Long> when;
    private final Long traversalSize;
    private List<Actor<Atomic>> plannedAtomics;

    public Rule(final Actor<Rule> self, final List<Long> when,
                final Long traversalSize) {
        super(self, Rule.class.getSimpleName() + "(pattern:" + when + ")");
        this.when = when;
        this.traversalSize = traversalSize;
        this.plannedAtomics = new ArrayList<>();
    }

    @Override
    public Either<Request, Response> receiveRequest(final Request fromUpstream, final ResponseProducer responseProducer) {
        assert fromUpstream.plan().atEnd() : "A rule that receives a fromUpstream must be at the end of the plan";

        Plan responsePlan = respondingPlan(fromUpstream);

        if (responseProducer.getOneTraversalProducer() != null) {
            List<Long> answers = produceTraversalAnswer(responseProducer);
            return Either.second(
                    new Response.Answer(fromUpstream, responsePlan, self(), answers, fromUpstream.constraints(), fromUpstream.unifiers()));
        } else if (responseProducer.hasReadyDownstreamRequest()) {
            return Either.first(responseProducer.getReadyDownstreamRequest());
        } else {
            return Either.second(new Response.Exhausted(fromUpstream, responsePlan, self()));
        }
    }

    @Override
    public Either<Request, Response> receiveAnswer(final Request fromUpstream, final Response.Answer fromDownstream, final ResponseProducer responseProducer) {

        Actor<? extends ExecutionActor<?>> downstream = fromDownstream.downstream();
        if (isLast(downstream)) {

            // TODO unify and materialise

            Plan upstreamPlan = upstreamPlan(fromDownstream);
            List<Long> newAnswer = fromDownstream.partialAnswer();
            return Either.second(
                    new Response.Answer(fromUpstream, upstreamPlan, self(), newAnswer, fromUpstream.constraints(), fromUpstream.unifiers()));
        } else {
            Actor<Atomic> nextPlannedDownstream = nextPlannedDownstream(downstream);
            Request downstreamRequest = new Request(nextPlannedDownstream, null, fromDownstream.partialAnswer(), fromDownstream.constraints(), fromDownstream.unifiers());
            return Either.first(downstreamRequest);
        }
    }

    @Override
    public Either<Request, Response> receiveExhausted(final Request fromUpstream, final Response.Exhausted fromDownstream, final ResponseProducer responseProducer) {
        Actor<? extends ExecutionActor<?>> downstream = fromDownstream.downstream();

        if (isFirst(downstream)) {
            // every rule has exactly 1 ready downstream, so an exhausted message must indicate the downstream is exhausted
            responseProducer.removeReadyDownstream(fromDownstream.sourceRequest());

            if (responseProducer.getOneTraversalProducer() != null) {
                List<Long> answers = produceTraversalAnswer(responseProducer);
                return Either.second(
                        new Response.Answer(fromUpstream, null, self(), answers, fromUpstream.constraints(), fromUpstream.unifiers()));
            } else {
                return Either.second(new Response.Exhausted(fromUpstream, null, self()));
            }
        } else {
            return Either.first(new Request(plannedAtomics.get(0), null, fromUpstream.partialAnswer(), fromUpstream.constraints(), fromUpstream.unifiers()));
        }
    }

    @Override
    protected ResponseProducer createResponseProducer(final Request request) {
        ResponseProducer responseProducer = new ResponseProducer();
        Plan nextPlan = request.plan().addSteps(this.plannedAtomics).toNextStep();
        Request toDownstream = new Request(nextPlan, request.partialAnswer(), request.constraints(), request.unifiers() );
        responseProducer.addReadyDownstream(toDownstream);

        Long startingAnswer = when.stream().reduce((acc, val) -> acc + val).get();
        Iterator<Long> traversal = (new MockTransaction(traversalSize, 1)).query(startingAnswer);
        if (traversal.hasNext()) responseProducer.addTraversalProducer(traversal);
        return responseProducer;
    }

    @Override
    protected void initialiseDownstreamActors(Registry registry) {
        List<Long> planned = new ArrayList<>(when);
        Collections.reverse(planned);
        planned = Collections.unmodifiableList(planned);
        // in the future, we'll check if the atom is rule resolvable first
        for (Long atomicPattern : planned) {
            Actor<Atomic> atomicActor = registry.registerAtomic(atomicPattern, (pattern) ->
                    child((newActor) -> new Atomic(newActor, pattern, 5L, Arrays.asList())));
            plannedAtomics.add(atomicActor);
        }
    }

    private List<Long> produceTraversalAnswer(final ResponseProducer responseProducer) {
        Iterator<Long> traversalProducer = responseProducer.getOneTraversalProducer();
        Long answer = traversalProducer.next();
        if (!traversalProducer.hasNext()) responseProducer.removeTraversalProducer(traversalProducer);
        return Arrays.asList(answer);
    }

    private Plan respondingPlan(final Request fromUpstream) {
        return fromUpstream.plan().endStepCompleted();
    }

    private Plan upstreamPlan(final Response.Answer fromDownstream) {
        return fromDownstream.plan().endStepCompleted();
    }

    private boolean isFirst(Actor<? extends ExecutionActor<?>>  actor) {
        return plannedAtomics.get(0).equals(actor);
    }

    private boolean isLast(Actor<? extends ExecutionActor<?>>  actor) {
        return plannedAtomics.get(plannedAtomics.size() - 1).equals(actor);
    }

    private Actor<Atomic> nextPlannedDownstream(Actor<? extends ExecutionActor<?>>  actor) {
        return plannedAtomics.get(plannedAtomics.indexOf(actor) + 1);
    }
}
