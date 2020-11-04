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

        if (!this.responseProducers.containsKey(fromUpstream)) {
            this.responseProducers.put(fromUpstream, initialiseResponseProducer(fromUpstream));
        }

        ResponseProducer responseProducer = this.responseProducers.get(fromUpstream);
        Plan responsePlan = fromUpstream.plan.endStepCompleted();

        if (responseProducer.finished()) {
            respondDoneToUpstream(fromUpstream, responsePlan);
        } else {
            // TODO if we want batching, we increment by as many as are requested
            responseProducer.requestsFromUpstream++;

            if (responseProducer.requestsFromUpstream > responseProducer.requestsToDownstream + responseProducer.answers.size()) {
                respondAnswersToUpstream(
                        fromUpstream,
                        responsePlan,
                        fromUpstream.partialAnswers,
                        fromUpstream.constraints,
                        fromUpstream.unifiers,
                        responseProducer,
                        responsePlan.currentStep()
                );
            }

            if (responseProducer.requestsFromUpstream > responseProducer.requestsToDownstream + responseProducer.answers.size()) {
                if (!responseProducer.isDownstreamDone()) {
                    requestFromDownstream(fromUpstream);
                }
            }
        }
    }

    @Override
    public void receiveAnswer(final Response.Answer answer) {
        LOG.debug("Received answer response in: " + name);
        Request request = answer.request();
        Request fromUpstream = requestRouter.get(request);
        ResponseProducer responseProducer = responseProducers.get(fromUpstream);
        responseProducer.requestsToDownstream--;

        Long mergedAnswer = answer.partialAnswers.stream().reduce(0L, (acc, val) -> acc + val);
        responseProducer.answers.add(mergedAnswer);

        Plan responsePlan = answer.plan.endStepCompleted();
        respondAnswersToUpstream(
                fromUpstream,
                responsePlan,
                fromUpstream.partialAnswers,
                fromUpstream.constraints,
                fromUpstream.unifiers,
                responseProducer,
                responsePlan.currentStep()
        );

        // TODO unify and materialise
    }

    @Override
    public void receiveDone(final Response.Done done) {
        LOG.debug("Received done response in: " + name);
        Request request = done.request();
        Request fromUpstream = requestRouter.get(request);
        ResponseProducer responseProducer = responseProducers.get(fromUpstream);
        responseProducer.requestsToDownstream--;

        responseProducer.downstreamDone(request);
        Plan responsePlan = done.plan.endStepCompleted();

        if (responseProducer.finished()) {
            respondDoneToUpstream(fromUpstream, responsePlan);
        } else {
            respondAnswersToUpstream(
                    fromUpstream,
                    responsePlan,
                    fromUpstream.partialAnswers,
                    fromUpstream.constraints,
                    fromUpstream.unifiers,
                    responseProducer,
                    responsePlan.currentStep()
            );
        }
    }

    @Override
    void requestFromDownstream(final Request fromUpstream) {
        Request toDownstream = responseProducers.get(fromUpstream).toDownstream();

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

    private ResponseProducer initialiseResponseProducer(final Request request) {
        ResponseProducer responseProducer = new ResponseProducer();
        Plan nextStep = request.plan.addStep(whenActor).toNextStep();
        Request toDownstream = new Request(
                nextStep,
                request.partialAnswers,
                request.constraints,
                request.unifiers
        );
        responseProducer.addAvailableDownstream(toDownstream);
        return responseProducer;
    }
}
