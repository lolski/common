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
    private final Map<Request, ResponseProducer> requestProducers;
    private final Map<Request, Request> requestRouter;
    private final String name;

    public RuleActor(final Actor<RuleActor> self, ActorRegistry actorRegistry, List<Long> when,
                     Long whenTraversalSize) {
        super(self, actorRegistry);
        LOG = LoggerFactory.getLogger(RuleActor.class.getSimpleName() + "-" + when);

        this.name = String.format("RuleActor(pattern:%s)", when);
        whenActor = child((newActor) -> new ConjunctiveActor(newActor, actorRegistry, when, whenTraversalSize, null));
        requestRouter = new HashMap<>();
        requestProducers = new HashMap<>();
    }

    @Override
    public void receiveRequest(final Request request) {
        assert request.plan.atEnd() : "A rule that receives a request must be at the end of the plan";

        if (!this.requestProducers.containsKey(request)) {
            this.requestProducers.put(request, new ResponseProducer(true));
        }

        ResponseProducer responseProducer = this.requestProducers.get(request);
        Plan responsePlan = request.plan.endStepCompleted();

        if (responseProducer.finished()) {
            respondDoneToUpstream(request, responsePlan);
        } else {
            // TODO if we want batching, we increment by as many as are requested
            responseProducer.requestsFromUpstream++;

            if (responseProducer.requestsFromUpstream > responseProducer.requestsToDownstream + responseProducer.answers.size()) {
                respondAnswersToUpstream(
                        request,
                        responsePlan,
                        request.partialAnswers,
                        request.constraints,
                        request.unifiers,
                        responseProducer,
                        responsePlan.currentStep()
                );
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
        Request request = answer.request();
        Request parentRequest = requestRouter.get(request);
        ResponseProducer responseProducer = requestProducers.get(parentRequest);
        responseProducer.requestsToDownstream--;
        Plan responsePlan = answer.plan.endStepCompleted();
        respondAnswersToUpstream(
                request,
                responsePlan,
                request.partialAnswers,
                request.constraints,
                request.unifiers,
                responseProducer,
                responsePlan.currentStep()
        );

        // TODO unify and materialise
    }

    @Override
    public void receiveDone(final Response.Done done) {
        LOG.debug("Received done response in: " + name);
        Request request = done.request();
        Request parentRequest = requestRouter.get(request);
        ResponseProducer responseProducer = requestProducers.get(parentRequest);
        responseProducer.requestsToDownstream--;

        responseProducer.setDownstreamDone();
        Plan responsePlan = done.plan.endStepCompleted();

        if (responseProducer.finished()) {
            respondDoneToUpstream(parentRequest, responsePlan);
        } else {
            respondAnswersToUpstream(
                    request,
                    responsePlan,
                    request.partialAnswers,
                    request.constraints,
                    request.unifiers,
                    responseProducer,
                    responsePlan.currentStep()
            );
        }
    }

    @Override
    void requestFromDownstream(final Request request) {
        Plan extendedPlan = request.plan.addStep(whenActor);
        Plan nextStep = extendedPlan.toNextStep();
        Request subrequest = new Request(
                nextStep,
                request.partialAnswers,
                request.constraints,
                request.unifiers
        );

        // TODO we may overwrite if multiple identical requests are sent, when to clean up?
        requestRouter.put(subrequest, request);

        LOG.debug("Requesting from downstream in: " + name);
        whenActor.tell(actor -> actor.receiveRequest(subrequest));
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
        // send as many answers as possible to requester
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
}
