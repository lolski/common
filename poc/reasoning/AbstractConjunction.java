package grakn.common.poc.reasoning;

import grakn.common.collection.Either;
import grakn.common.concurrent.actor.Actor;
import grakn.common.poc.reasoning.execution.ExecutionActor;
import grakn.common.poc.reasoning.execution.Request;
import grakn.common.poc.reasoning.execution.Response;
import grakn.common.poc.reasoning.execution.ResponseProducer;
import grakn.common.poc.reasoning.mock.MockTransaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import static grakn.common.collection.Collections.concat;

public class AbstractConjunction<T extends AbstractConjunction<T>> extends ExecutionActor<T> {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractConjunction.class);

    private final Long traversalSize;
    private final List<Long> conjunction;
    private final List<Actor<Atomic>> plannedAtomics;

    public AbstractConjunction(final Actor<T> self, String name, final List<Long> conjunction, final Long traversalSize ,final LinkedBlockingQueue<List<Long>> responses) {
        super(self, name, responses);

        this.conjunction = conjunction;
        this.traversalSize = traversalSize;
        this.plannedAtomics = new ArrayList<>();
    }

    @Override
    public Either<Request, Response> receiveRequest(final Request fromUpstream, final ResponseProducer responseProducer) {
        return produce(fromUpstream, responseProducer);
    }

    @Override
    public Either<Request, Response> receiveAnswer(final Request fromUpstream, final Response.Answer fromDownstream, ResponseProducer responseProducer) {
        Actor<? extends ExecutionActor<?>> sender = fromDownstream.sourceRequest().receiver();
        List<Long> answer = concat(conjunction, fromDownstream.partialAnswer());
        if (isLast(sender)) {
            LOG.debug(this.name + ": hasProduced: " + answer);

            if (!responseProducer.hasProduced(answer)) {
                responseProducer.recordProduced(answer);
                return Either.second(
                        new Response.Answer(fromUpstream, answer, fromUpstream.constraints(), fromUpstream.unifiers()));
            } else {
                return produce(fromUpstream, responseProducer);
            }
        } else {
            Actor<Atomic> nextPlannedDownstream = nextPlannedDownstream(sender);
            Request downstreamRequest = new Request(fromUpstream.path().append(nextPlannedDownstream),
                    answer, fromDownstream.constraints(), fromDownstream.unifiers());
            responseProducer.addReadyDownstream(downstreamRequest);
            return Either.first(downstreamRequest);
        }
    }

    @Override
    public Either<Request, Response> receiveExhausted(final Request fromUpstream, final Response.Exhausted fromDownstream, final ResponseProducer responseProducer) {
        responseProducer.removeReadyDownstream(fromDownstream.sourceRequest());

        return produce(fromUpstream, responseProducer);
    }

    @Override
    protected ResponseProducer createResponseProducer(final Request request) {
        Iterator<List<Long>> traversal = (new MockTransaction(traversalSize, 0L, 1)).query(conjunction);
        ResponseProducer responseProducer = new ResponseProducer(traversal);
        Request toDownstream = new Request(request.path().append(plannedAtomics.get(0)), request.partialAnswer(),
                request.constraints(), request.unifiers());
        responseProducer.addReadyDownstream(toDownstream);

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

    private Either<Request, Response> produce(Request fromUpstream, ResponseProducer responseProducer) {
        while (responseProducer.hasTraversalProducer()) {
            List<Long> answer = traverseOnce(responseProducer);
            LOG.debug(this.name + ": hasProduced: " + answer);
            if (!responseProducer.hasProduced(answer)) {
                responseProducer.recordProduced(answer);
                return Either.second(new Response.Answer(fromUpstream, answer, fromUpstream.constraints(), fromUpstream.unifiers()));
            }
        }

        if (responseProducer.hasReadyDownstreamRequest()) {
            return Either.first(responseProducer.getReadyDownstreamRequest());
        } else {
            return Either.second(new Response.Exhausted(fromUpstream));
        }
    }

    private List<Long> traverseOnce(final ResponseProducer responseProducer) {
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
