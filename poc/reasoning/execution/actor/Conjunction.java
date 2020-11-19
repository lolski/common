package grakn.common.poc.reasoning.execution.actor;

import grakn.common.collection.Either;
import grakn.common.concurrent.actor.Actor;
import grakn.common.poc.reasoning.execution.AnswerRecorder;
import grakn.common.poc.reasoning.execution.framework.Answer;
import grakn.common.poc.reasoning.execution.framework.ExecutionRecord;
import grakn.common.poc.reasoning.execution.framework.Request;
import grakn.common.poc.reasoning.execution.framework.Response;
import grakn.common.poc.reasoning.mock.MockTransaction;
import grakn.common.poc.reasoning.execution.framework.ExecutionActor;
import grakn.common.poc.reasoning.execution.Registry;
import grakn.common.poc.reasoning.execution.framework.ResponseProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import static grakn.common.collection.Collections.concat;
import static grakn.common.collection.Collections.copy;
import static grakn.common.collection.Collections.map;
import static grakn.common.collection.Collections.set;

public class Conjunction<T extends Conjunction<T>> extends ExecutionActor<T> {
    private static final Logger LOG = LoggerFactory.getLogger(Conjunction.class);

    private final Long traversalSize;
    private final Long traversalOffset;
    private final List<Long> conjunction;
    private final List<Actor<Concludable>> plannedConcludables;
    private Actor<AnswerRecorder> executionRecorder;

    public Conjunction(Actor<T> self, String name, List<Long> conjunction, Long traversalSize,
                       Long traversalOffset, LinkedBlockingQueue<Response> responses) {
        super(self, name, responses);

        this.conjunction = conjunction;
        this.traversalSize = traversalSize;
        this.traversalOffset = traversalOffset;
        this.plannedConcludables = new ArrayList<>();
    }

    @Override
    public Either<Request, Response> receiveRequest(Request fromUpstream, ResponseProducer responseProducer) {
        return produceMessage(fromUpstream, responseProducer);
    }

    @Override
    public Either<Request, Response> receiveAnswer(Request fromUpstream, Response.Answer fromDownstream, ResponseProducer responseProducer) {
        Actor<? extends ExecutionActor<?>> sender = fromDownstream.sourceRequest().receiver();
        List<Long> conceptMap = concat(conjunction, fromDownstream.answer().conceptMap());

        ExecutionRecord executionRecord = fromDownstream.sourceRequest().partialResolutions();
        if (fromDownstream.answer().isInferred()) {
            executionRecord = executionRecord.withAnswer(fromDownstream.sourceRequest().receiver(), fromDownstream.answer());
        }

        if (isLast(sender)) {
            LOG.debug("{}: hasProduced: {}", name, conceptMap);

            if (!responseProducer.hasProduced(conceptMap)) {
                responseProducer.recordProduced(conceptMap);

                Answer answer = new Answer(conceptMap, conjunction.toString(), executionRecord, self());
                Response.Answer response = new Response.Answer(fromUpstream, answer, fromUpstream.unifiers());
                if (fromUpstream.sender() == null) {
                    executionRecorder.tell(state -> state.record(answer));
                }
                return Either.second(response);
            } else {
                return produceMessage(fromUpstream, responseProducer);
            }
        } else {
            Actor<Concludable> nextPlannedDownstream = nextPlannedDownstream(sender);
            Request downstreamRequest = new Request(fromUpstream.path().append(nextPlannedDownstream),
                    conceptMap, fromDownstream.unifiers(), executionRecord);
            responseProducer.addDownstreamProducer(downstreamRequest);
            return Either.first(downstreamRequest);
        }
    }

    @Override
    public Either<Request, Response> receiveExhausted(Request fromUpstream, Response.Exhausted fromDownstream, ResponseProducer responseProducer) {
        responseProducer.removeDownstreamProducer(fromDownstream.sourceRequest());

        return produceMessage(fromUpstream, responseProducer);
    }

    @Override
    protected ResponseProducer createResponseProducer(Request request) {
        Iterator<List<Long>> traversal = (new MockTransaction(traversalSize, traversalOffset, 1)).query(conjunction);
        ResponseProducer responseProducer = new ResponseProducer(traversal);
        Request toDownstream = new Request(request.path().append(plannedConcludables.get(0)), request.partialConceptMap(),
                request.unifiers(), new ExecutionRecord(map()));
        responseProducer.addDownstreamProducer(toDownstream);

        return responseProducer;
    }

    @Override
    protected void initialiseDownstreamActors(Registry registry) {
        executionRecorder = registry.executionRecorder();
        List<Long> planned = copy(conjunction);
        // in the future, we'll check if the atom is rule resolvable first
        for (Long atomicPattern : planned) {
            Actor<Concludable> atomicActor = registry.registerConcludable(atomicPattern, (pattern) ->
                    Actor.create(self().eventLoopGroup(), (newActor) -> new Concludable(newActor, pattern, Arrays.asList(), 5L)));
            plannedConcludables.add(atomicActor);
        }
    }

    private Either<Request, Response> produceMessage(Request fromUpstream, ResponseProducer responseProducer) {
        while (responseProducer.hasTraversalProducer()) {
            List<Long> conceptMap = responseProducer.traversalProducer().next();
            LOG.debug("{}: hasProduced: {}", name, conceptMap);
            if (!responseProducer.hasProduced(conceptMap)) {
                responseProducer.recordProduced(conceptMap);
                Answer answer = new Answer(conceptMap, conjunction.toString(), ExecutionRecord.EMPTY, self());
                return Either.second(new Response.Answer(fromUpstream, answer, fromUpstream.unifiers()));
            }
        }

        if (responseProducer.hasDownstreamProducer()) {
            return Either.first(responseProducer.nextDownstreamProducer());
        } else {
            return Either.second(new Response.Exhausted(fromUpstream));
        }
    }

    private boolean isLast(Actor<? extends ExecutionActor<?>>  actor) {
        return plannedConcludables.get(plannedConcludables.size() - 1).equals(actor);
    }

    private Actor<Concludable> nextPlannedDownstream(Actor<? extends ExecutionActor<?>>  actor) {
        return plannedConcludables.get(plannedConcludables.indexOf(actor) + 1);
    }

    @Override
    protected void exception(Exception e) {
        LOG.error("Actor exception", e);
        // TODO, once integrated into the larger flow of executing queries, kill the actors and report and exception to root
    }
}
