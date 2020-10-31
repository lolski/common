package grakn.common.poc.reasoning;

import grakn.common.concurrent.actor.Actor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class ConjunctiveActor extends Actor.State<ConjunctiveActor> implements ReasoningActor {
    private final String name;
    Logger LOG;

    private final List<Long> conjunction;
    @Nullable
    private final LinkedBlockingQueue<Long> responses;
    private final Map<Request, ResponseProducer> requestProducers;
    private final Map<Request, Request> requestRouter; // TODO note that this can be many to one, and is not catered for yet (ie. request followed the same request)


    protected ConjunctiveActor(final Actor<ConjunctiveActor> self, ActorManager manager, List<Long> conjunction, LinkedBlockingQueue<Long> responses) throws InterruptedException {
        super(self);
        LOG = LoggerFactory.getLogger(ConjunctiveActor.class.getSimpleName() + "-" + conjunction);

        this.name = "Conjunction-" + conjunction;
        this.conjunction = conjunction;
        this.responses = responses;

        List<Actor<AtomicActor>> reverseOrderedActors = new ArrayList<>();
        // TODO make generics so that this accepts all reasoning actors
//        reverseOrderedActors.add(self);
        List<Long> plan = plan(this.conjunction);
        Collections.reverse(plan);
        // in the future, we'll check if the atom is rule resolvable first
        for (Long atomicConstraint : plan) {
            Actor<AtomicActor> actor = manager.getAtomicActor(atomicConstraint);
            if (actor == null) actor = manager.createAtomicActor(atomicConstraint, 5L);
            reverseOrderedActors.add(actor);
        }

        Path path = new Path(reverseOrderedActors);

        requestProducers = new HashMap<>();
        requestRouter = new HashMap<>();
    }

    @Override
    public void receiveRequest(final Request request) {
        LOG.debug("Received request in: " + name);
        if (!this.requestProducers.containsKey(request)) {
            boolean downstreamExists = request.path.directDownstream() != null;
            this.requestProducers.put(request, new ResponseProducer(downstreamExists));
        }

        ResponseProducer responseProducer = this.requestProducers.get(request);
        if (responseProducer.finished()) {
            respondDoneToRequester(request);
        } else {
            // TODO if we want batching, we increment by as many as are requested
            responseProducer.requestsFromUpstream++;

            if (responseProducer.requestsFromUpstream > responseProducer.requestsToDownstream + responseProducer.answers.size()) {
                respondAnswersToRequester(request, responseProducer);
            }

            if (responseProducer.requestsFromUpstream > responseProducer.requestsToDownstream + responseProducer.answers.size()) {
                if (!responseProducer.isDownstreamDone()) {
                    requestFromDownstream(request);
                }
            }
        }
    }

    @Override
    public void receiveAnswer(final Response.Answer answer) {
        LOG.debug("Received answer response in: " + name);

    }

    @Override
    public void receiveDone(final Response.Done done) {
        LOG.debug("Received done response in: " + name);

    }

    private void requestFromDownstream(final Request request) {
        Actor<AtomicActor> downstream = request.path.directDownstream();
        Path downstreamPath = request.path.moveDownstream();
        Request subrequest = new Request(
                downstreamPath,
                request.partialAnswers,
                request.constraints,
                request.unifiers
        );

        // TODO we may overwrite if multiple identical requests are sent, when to clean up?
        requestRouter.put(subrequest, request);

        LOG.debug("Requesting from downstream from: " + name);
        downstream.tell(actor -> actor.receiveRequest(subrequest));
        requestProducers.get(request).requestsToDownstream++;
    }

    private void respondAnswersToRequester(final Request request, final ResponseProducer responseProducer) {
        // send as many answers as possible to requester
        for (int i = 0; i < Math.min(responseProducer.requestsFromUpstream, responseProducer.answers.size()); i++) {
            Long answer = responseProducer.answers.remove(0);
            if (request.path.atRoot()) {
                // base case - how to return from Actor model
                assert responses != null : this + ": can't return answers because the user answers queue is null";
                LOG.debug("Saving answer to output queue in actor: " + name);
                responses.add(answer);
            } else {
                // TODO
            }
            responseProducer.requestsFromUpstream--;
        }
    }

    private void respondDoneToRequester(final Request request) {
        if (request.path.atRoot()) {
            // base case - how to return from Actor model
            assert responses != null : this + ": can't return answers because the user answers queue is null";
            LOG.debug("Sending DONE response to output from actor: " + name);
            responses.add(-1L);
        } else {
            // TODO
        }
    }


    /*
    Given a conjunction, return an ordered list of constraints to traverse
    The first constraint should be the starting point that finds initial answers
    before propagating them in the order indicated by the plan
     */
    private List<Long> plan(List<Long> conjunction) {
        // plan the atomics in the conjunction
        return conjunction;
    }
}
