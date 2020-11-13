package grakn.common.poc.reasoning;

import grakn.common.collection.Either;
import grakn.common.concurrent.actor.Actor;
import grakn.common.poc.reasoning.execution.ExecutionActor;
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
    private final List<Actor<Atomic>> plannedAtomics;

    public Rule(final Actor<Rule> self, final List<Long> when,
                final Long traversalSize) {
        super(self, Rule.class.getSimpleName() + "(pattern:" + when + ")");
        this.when = when;
        this.traversalSize = traversalSize;
        this.plannedAtomics = new ArrayList<>();
    }

    @Override
    public Either<Request, Response> receiveRequest(final Request fromUpstream, final ResponseProducer responseProducer) {
        if (responseProducer.hasTraversalProducer()) {
            List<Long> answer = produceTraversalAnswer(responseProducer);
            return Either.second(
                    new Response.Answer(fromUpstream, answer, fromUpstream.constraints(), fromUpstream.unifiers()));
        } else if (responseProducer.hasReadyDownstreamRequest()) {
            return Either.first(responseProducer.getReadyDownstreamRequest());
        } else {
            return Either.second(new Response.Exhausted(fromUpstream));
        }
    }

    @Override
    public Either<Request, Response> receiveAnswer(final Request fromUpstream, final Response.Answer fromDownstream, final ResponseProducer responseProducer) {
        Actor<? extends ExecutionActor<?>> sender = fromDownstream.sourceRequest().receiver();

        if (isLast(sender)) {
            // TODO unify and materialise
            // TODO: deduplicate answer: if the answer that you want to send up is already sent before, retry or send done if all downstreams are exhausted
            List<Long> newAnswer = fromDownstream.partialAnswer();
            return Either.second(
                    new Response.Answer(fromUpstream, newAnswer, fromUpstream.constraints(), fromUpstream.unifiers()));
        } else {
            Actor<Atomic> nextPlannedDownstream = nextPlannedDownstream(sender);
            Request downstreamRequest = new Request(fromUpstream.path().append(nextPlannedDownstream),
                    fromDownstream.partialAnswer(), fromDownstream.constraints(), fromDownstream.unifiers());
            responseProducer.addReadyDownstream(downstreamRequest);
            return Either.first(downstreamRequest);
        }
    }

    @Override
    public Either<Request, Response> receiveExhausted(final Request fromUpstream, final Response.Exhausted fromDownstream, final ResponseProducer responseProducer) {
        responseProducer.removeReadyDownstream(fromDownstream.sourceRequest());

        if (responseProducer.hasTraversalProducer()) {
            List<Long> answer = produceTraversalAnswer(responseProducer);
            return Either.second(
                    new Response.Answer(fromUpstream, answer, fromUpstream.constraints(), fromUpstream.unifiers()));
        } else if (responseProducer.hasReadyDownstreamRequest()) {
            return Either.first(responseProducer.getReadyDownstreamRequest());
        } else {
            return Either.second(new Response.Exhausted(fromUpstream));
        }
    }

    @Override
    protected ResponseProducer createResponseProducer(final Request request) {
        Iterator<List<Long>> traversal = (new MockTransaction(traversalSize, 0L, 1)).query(when);
        ResponseProducer responseProducer = new ResponseProducer(traversal);
        Request toDownstream = new Request(request.path().append(plannedAtomics.get(0)), request.partialAnswer(),
                request.constraints(), request.unifiers());
        responseProducer.addReadyDownstream(toDownstream);

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
        Iterator<List<Long>> traversalProducer = responseProducer.traversalProducer();
        return traversalProducer.next();
    }

    private boolean isLast(Actor<? extends ExecutionActor<?>>  actor) {
        return plannedAtomics.get(plannedAtomics.size() - 1).equals(actor);
    }

    private Actor<Atomic> nextPlannedDownstream(Actor<? extends ExecutionActor<?>>  actor) {
        return plannedAtomics.get(plannedAtomics.indexOf(actor) + 1);
    }
}
