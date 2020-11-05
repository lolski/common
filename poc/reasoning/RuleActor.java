package grakn.common.poc.reasoning;

import grakn.common.concurrent.actor.Actor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class RuleActor extends ReasoningActor<RuleActor> {
    private final Logger LOG;

    private final String name;
    private final Actor<ConjunctiveActor> whenActor;

    public RuleActor(final Actor<RuleActor> self, final ActorRegistry actorRegistry, final List<Long> when,
                     final Long whenTraversalSize) {
        super(self, actorRegistry);
        LOG = LoggerFactory.getLogger(RuleActor.class.getSimpleName() + "-" + when);

        name = String.format("RuleActor(pattern:%s)", when);
        whenActor = child((newActor) -> new ConjunctiveActor(newActor, actorRegistry, when, whenTraversalSize, null));
    }

    @Override
    public void receiveRequest(final Request fromUpstream) {
        LOG.debug("Received fromUpstream in: " + name);
        assert fromUpstream.plan.atEnd() : "A rule that receives a fromUpstream must be at the end of the plan";

        initialiseResponseProducer(fromUpstream);

        Plan responsePlan = getResponsePlan(fromUpstream);

        if (noMoreAnswersPossible(fromUpstream)) respondExhaustedToUpstream(fromUpstream, responsePlan);
        else {
            // TODO if we want batching, we increment by as many as are requested
            incrementRequestsFromUpstream(fromUpstream);

            if (upstreamHasRequestsOutstanding(fromUpstream) && downstreamAvailable(fromUpstream)) {
                requestFromAvailableDownstream(fromUpstream);
            }
        }
    }

    @Override
    public void receiveAnswer(final Response.Answer fromDownstream) {
        LOG.debug("Received answer response in: " + name);
        Request sentDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = requestRouter.get(sentDownstream);

        decrementRequestToDownstream(fromUpstream);
        Plan forwardingPlan = forwardingPlan(fromDownstream);

        respondAnswerToUpstream(
                fromUpstream,
                forwardingPlan,
                fromUpstream.partialAnswers,
                fromUpstream.constraints,
                fromUpstream.unifiers,
                responseProducers.get(fromUpstream),
                forwardingPlan.currentStep()
        );

        // TODO unify and materialise
    }

    @Override
    public void receiveExhausted(final Response.Exhausted fromDownstream) {
        LOG.debug("Received exhausted response in: " + name);
        Request sentDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = requestRouter.get(sentDownstream);
        decrementRequestToDownstream(fromUpstream);

        downstreamExhausted(fromUpstream, sentDownstream);
        Plan responsePlan = getResponsePlan(fromUpstream);

        if (noMoreAnswersPossible(fromUpstream))  respondExhaustedToUpstream(fromUpstream, responsePlan);
    }

    @Override
    void requestFromAvailableDownstream(final Request fromUpstream) {
        Request toDownstream = responseProducers.get(fromUpstream).getAvailableDownstream();

        // TODO we may overwrite if multiple identical requests are sent, when to clean up?
        requestRouter.put(toDownstream, fromUpstream);

        LOG.debug("Requesting from downstream in: " + name);
        whenActor.tell(actor -> actor.receiveRequest(toDownstream));
    }

    @Override
    void respondAnswerToUpstream(
            final Request request,
            final Plan plan,
            final List<Long> partialAnswer,
            final List<Object> constraints,
            final List<Object> unifiers,
            final ResponseProducer responseProducer,
            final Actor<? extends ReasoningActor<?>> upstream
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
        Actor<? extends ReasoningActor<?>> upstream = responsePlan.currentStep();
        Response.Exhausted responseExhausted = new Response.Exhausted(request, responsePlan);
        LOG.debug("Responding Exhausted to upstream in: " + name);
        upstream.tell((actor) -> actor.receiveExhausted(responseExhausted));
    }

    private void initialiseResponseProducer(final Request request) {
        if (!responseProducers.containsKey(request)) {
            ResponseProducer responseProducer = new ResponseProducer();
            responseProducers.put(request, responseProducer);
            Plan nextStep = request.plan.addStep(whenActor).toNextStep();
            Request toDownstream = new Request(
                    nextStep,
                    request.partialAnswers,
                    request.constraints,
                    request.unifiers
            );
            responseProducer.addAvailableDownstream(toDownstream);
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

    private void decrementRequestToDownstream(final Request parentRequest) {
        responseProducers.get(parentRequest).decrementRequestsToDownstream();
    }

    private Plan getResponsePlan(final Request fromUpstream) {
        return fromUpstream.plan.endStepCompleted();
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
