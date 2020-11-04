package grakn.common.poc.reasoning;

import grakn.common.concurrent.actor.Actor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RuleActor extends ReasoningActor<RuleActor> {
    private final Logger LOG;

    private final String name;
    private final Actor<ConjunctiveActor> whenActor;
    private final Map<Request, ResponseProducer> responseProducers;
    private final Map<Request, Request> requestRouter;

    public RuleActor(final Actor<RuleActor> self, final ActorRegistry actorRegistry, final List<Long> when,
                     final Long whenTraversalSize) {
        super(self, actorRegistry);
        LOG = LoggerFactory.getLogger(RuleActor.class.getSimpleName() + "-" + when);

        name = String.format("RuleActor(pattern:%s)", when);
        whenActor = child((newActor) -> new ConjunctiveActor(newActor, actorRegistry, when, whenTraversalSize, null));
        requestRouter = new HashMap<>();
        responseProducers = new HashMap<>();
    }

    @Override
    public void receiveRequest(final Request fromUpstream) {
        LOG.debug("Received fromUpstream in: " + name);
        assert fromUpstream.plan.atEnd() : "A rule that receives a fromUpstream must be at the end of the plan";

        initialiseResponseProducer(fromUpstream);

        Plan responsePlan = getResponsePlan(fromUpstream);

        if (noMoreAnswersPossible(fromUpstream)) respondDoneToUpstream(fromUpstream, responsePlan);
        else {
            // TODO if we want batching, we increment by as many as are requested
            incrementRequestsFromUpstream(fromUpstream);

            if (upstreamHasRequestsOutstanding(fromUpstream)) {
                respondAnswersToUpstream(
                        fromUpstream,
                        responsePlan,
                        fromUpstream.partialAnswers,
                        fromUpstream.constraints,
                        fromUpstream.unifiers,
                        responseProducers.get(fromUpstream),
                        responsePlan.currentStep()
                );
            }

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

        Long mergedAnswer = computeAnswer(fromDownstream);
        bufferAnswer(fromUpstream, mergedAnswer);

        Plan forwardingPlan = forwardingPlan(fromDownstream);
        respondAnswersToUpstream(
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
    public void receiveDone(final Response.Done fromDownstream) {
        LOG.debug("Received done response in: " + name);
        Request sentDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = requestRouter.get(sentDownstream);
        decrementRequestToDownstream(fromUpstream);

        downstreamDone(fromUpstream, sentDownstream);
        Plan responsePlan = getResponsePlan(fromUpstream);

        if (noMoreAnswersPossible(fromUpstream)) {
            respondDoneToUpstream(fromUpstream, responsePlan);
        } else {
            respondAnswersToUpstream(
                    fromUpstream,
                    responsePlan,
                    fromUpstream.partialAnswers,
                    fromUpstream.constraints,
                    fromUpstream.unifiers,
                    responseProducers.get(fromUpstream),
                    responsePlan.currentStep()
            );
        }
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
    void respondAnswersToUpstream(
            final Request request,
            final Plan plan,
            final List<Long> partialAnswers,
            final List<Object> constraints,
            final List<Object> unifiers,
            final ResponseProducer responseProducer,
            final Actor<? extends ReasoningActor<?>> upstream
    ) {
        // send as many answers as possible to upstream
        for (int i = 0; i < Math.min(responseProducer.requestsFromUpstream, responseProducer.bufferedAnswersSize()); i++) {
            Long answer = responseProducer.bufferedAnswersTake();
            List<Long> newAnswers = new ArrayList<>(partialAnswers);
            newAnswers.add(answer);
            Response.Answer responseAnswer = new Response.Answer(
                    request,
                    plan,
                    newAnswers,
                    constraints,
                    unifiers
            );

            LOG.debug("Responding answer to upstream from actor: " + name);
            upstream.tell((actor) -> actor.receiveAnswer(responseAnswer));
            responseProducer.requestsFromUpstream--;
        }
    }

    @Override
    void respondDoneToUpstream(final Request request, final Plan responsePlan) {
        Actor<? extends ReasoningActor<?>> upstream = responsePlan.currentStep();
        Response.Done responseDone = new Response.Done(request, responsePlan);
        LOG.debug("Responding Done to upstream in: " + name);
        upstream.tell((actor) -> actor.receiveDone(responseDone));
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

    private void bufferAnswer(final Request request, final Long answer) {
        responseProducers.get(request).bufferedAnswersAdd(Arrays.asList(answer));
    }

    private boolean upstreamHasRequestsOutstanding(final Request fromUpstream) {
        ResponseProducer responseProducer = responseProducers.get(fromUpstream);
        return responseProducer.requestsFromUpstream > responseProducer.requestsToDownstream + responseProducer.bufferedAnswersSize();
    }

    private boolean noMoreAnswersPossible(final Request fromUpstream) {
        return responseProducers.get(fromUpstream).noMoreAnswersPossible();
    }

    private void incrementRequestsFromUpstream(final Request fromUpstream) {
        responseProducers.get(fromUpstream).requestsFromUpstream++;
    }

    private void decrementRequestToDownstream(final Request parentRequest) {
        responseProducers.get(parentRequest).requestsToDownstream--;
    }

    private Plan getResponsePlan(final Request fromUpstream) {
        return fromUpstream.plan.endStepCompleted();
    }

    private Plan forwardingPlan(final Response.Answer answer) {
        return answer.plan.endStepCompleted();
    }

    private boolean downstreamAvailable(final Request fromUpstream) {
        return !responseProducers.get(fromUpstream).downstreamDone();
    }

    private void downstreamDone(final Request fromUpstream, final Request sentDownstream) {
        responseProducers.get(fromUpstream).downstreamDone(sentDownstream);
    }

    private Long computeAnswer(final Response.Answer answer) {
        return answer.partialAnswers.stream().reduce(0L, (acc, val) -> acc + val);
    }
}
