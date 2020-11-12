package grakn.common.poc.reasoning;

import grakn.common.collection.Either;
import grakn.common.concurrent.actor.Actor;
import grakn.common.poc.reasoning.execution.ExecutionActor;
import grakn.common.poc.reasoning.execution.Plan;
import grakn.common.poc.reasoning.execution.Request;
import grakn.common.poc.reasoning.execution.Response;
import grakn.common.poc.reasoning.execution.ResponseProducer;
import grakn.common.poc.reasoning.mock.MockTransaction;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

public class Conjunction extends ExecutionActor<Conjunction> {
    private final Long traversalSize;
    @Nullable
    private final List<Long> conjunction;
    private List<Actor<Atomic>> plannedAtomics;

    public Conjunction(final Actor<Conjunction> self, final List<Long> conjunction,
                final Long traversalSize, final LinkedBlockingQueue<Long> responses) {
        super(self, Conjunction.class.getSimpleName() + "(pattern:" + conjunction + ")", responses);

        this.conjunction = conjunction;
        this.traversalSize = traversalSize;
        this.plannedAtomics = new ArrayList<>();
    }

    @Override
    public Either<Request, Response> receiveRequest(final Request fromUpstream, final ResponseProducer responseProducer) {
        assert fromUpstream.plan().atEnd() : "A conjunction that receives a request must be at the end of the plan";

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
        List<Long> newAnswer = fromDownstream.partialAnswer();
        return Either.second(
                new Response.Answer(fromUpstream, forwardingPlan, newAnswer, fromUpstream.constraints(), fromUpstream.unifiers()));
    }

    @Override
    public Either<Request, Response> receiveExhausted(final Request fromUpstream, final Response.Exhausted fromDownstream, final ResponseProducer responseProducer) {
        // every conjunction has exactly 1 downstream, so an exhausted message must indicate the downstream is exhausted
        responseProducer.downstreamExhausted(fromDownstream.sourceRequest());
        Plan responsePlan = respondingPlan(fromUpstream);

        if (responseProducer.getOneTraversalProducer() != null) {
            List<Long> answers = produceTraversalAnswer(responseProducer);
            return Either.second(
                    new Response.Answer(fromUpstream, responsePlan, answers, fromUpstream.constraints(), fromUpstream.unifiers()));
        } else {
            return Either.second(new Response.Exhausted(fromUpstream, responsePlan));
        }
    }

    @Override
    protected ResponseProducer createResponseProducer(final Request request) {
        ResponseProducer responseProducer = new ResponseProducer();

        Plan nextPlan = request.plan().addSteps(this.plannedAtomics).toNextStep();
        Request toDownstream = new Request(nextPlan, request.partialAnswer(), request.constraints(), request.unifiers());
        responseProducer.addAvailableDownstream(toDownstream);

        Long startingAnswer = conjunction.stream().reduce((acc, val) -> acc + val).get();
        Iterator<Long> traversal = (new MockTransaction(traversalSize, 1)).query(startingAnswer);
        if (traversal.hasNext()) responseProducer.addTraversalProducer(traversal);
        return responseProducer;
    }

    @Override
    protected void initialiseDownstreamActors(Registry registry) {
        List<Long> planned = new ArrayList<>(conjunction);
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
        // TODO could do batch traverse, or retrieve answers from multiple traversals
        Long answer = traversalProducer.next();
        if (!traversalProducer.hasNext()) responseProducer.removeTraversalProducer(traversalProducer);
        return Arrays.asList(answer);
    }

    private Plan respondingPlan(final Request fromUpstream) {
        return fromUpstream.plan().endStepCompleted();
    }

    private Plan forwardingPlan(final Response.Answer fromDownstream) {
        return fromDownstream.plan().endStepCompleted();
    }

}
