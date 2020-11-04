package grakn.common.poc.reasoning;

import grakn.common.concurrent.actor.Actor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RuleActor extends ReasoningActor<RuleActor> {
    Logger LOG;

    private final Actor<ConjunctiveActor> whenActor;
    private final Map<Request, ResponseProducer> responseProducers;
    private final Map<Request, Request> requestRouter;
    private final String name;

    public RuleActor(final Actor<RuleActor> self, ActorRegistry actorRegistry, List<Long> when,
                     Long whenTraversalSize) {
        super(self, actorRegistry);
        LOG = LoggerFactory.getLogger(RuleActor.class.getSimpleName() + "-" + when);

        this.name = String.format("RuleActor(pattern:%s)", when);
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

    private boolean downstreamAvailable(Request fromUpstream) {
        return !responseProducers.get(fromUpstream).isDownstreamDone();
    }

    private boolean upstreamHasRequestsOutstanding(Request fromUpstream) {
        ResponseProducer responseProducer = responseProducers.get(fromUpstream);
        return responseProducer.requestsFromUpstream > responseProducer.requestsToDownstream + responseProducer.answers.size();
    }

    private void incrementRequestsFromUpstream(Request fromUpstream) {
        responseProducers.get(fromUpstream).requestsFromUpstream++;
    }

    private boolean noMoreAnswersPossible(Request fromUpstream) {
        return responseProducers.get(fromUpstream).finished();
    }

    private Plan getResponsePlan(Request fromUpstream) {
        return fromUpstream.plan.endStepCompleted();
    }

    @Override
    public void receiveAnswer(final Response.Answer answer) {
        LOG.debug("Received answer response in: " + name);
        Request sentDownstream = answer.sourceRequest();
        Request fromUpstream = requestRouter.get(sentDownstream);

        decrementRequestToDownstream(fromUpstream);

        Long mergedAnswer = getAnswer(answer);
        bufferAnswer(fromUpstream, mergedAnswer);

        Plan forwardingPlan = forwardingPlan(answer);
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

    private Plan forwardingPlan(Response.Answer answer) {
        return answer.plan.endStepCompleted();
    }

    private Long getAnswer(Response.Answer answer) {
        return answer.partialAnswers.stream().reduce(0L, (acc, val) -> acc + val);
    }

    private void bufferAnswer(Request request, Long answer) {
        responseProducers.get(request).answers.add(answer);
    }

    private void decrementRequestToDownstream(Request parentRequest) {
        responseProducers.get(parentRequest).requestsToDownstream--;
    }

    @Override
    public void receiveDone(final Response.Done done) {
        LOG.debug("Received done response in: " + name);
        Request sentDownstream = done.sourceRequest();
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

    private void downstreamDone(final Request fromUpstream, final Request sentDownstream) {
        responseProducers.get(fromUpstream).downstreamDone(sentDownstream);
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
        for (int i = 0; i < Math.min(responseProducer.requestsFromUpstream, responseProducer.answers.size()); i++) {
            Long answer = responseProducer.answers.remove(0);
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
}
