package grakn.common.poc.reasoning;

import grakn.common.collection.Either;
import grakn.common.concurrent.actor.Actor;

import java.util.List;

public class RuleActor extends ExecutionActor<RuleActor> {
    private final Actor<ConjunctiveActor> whenActor;

    public RuleActor(final Actor<RuleActor> self, final ActorRegistry actorRegistry, final List<Long> when,
                     final Long whenTraversalSize) {
        super(self, actorRegistry, RuleActor.class.getSimpleName() + "(pattern:" + when + ")");
        whenActor = child((newActor) -> new ConjunctiveActor(newActor, actorRegistry, when, whenTraversalSize));
    }

    @Override
    public Either<Request, Response> receiveRequest(final Request fromUpstream, final ResponseProducer responseProducer) {
        assert fromUpstream.plan().atEnd() : "A rule that receives a fromUpstream must be at the end of the plan";

        Plan responsePlan = getResponsePlan(fromUpstream);

        if (!responseProducer.downstreamsExhausted()) {
            return Either.first(responseProducer.getAvailableDownstream());
        } else {
            return Either.second(new Response.Exhausted(fromUpstream, responsePlan));
        }
    }

    @Override
    public Either<Request, Response> receiveAnswer(final Request fromUpstream, final Response.Answer fromDownstream, final ResponseProducer responseProducer) {
        Plan forwardingPlan = forwardingPlan(fromDownstream);

        List<Long> newAnwser = fromDownstream.partialAnswers;

        // TODO unify and materialise

        return Either.second(
                new Response.Answer(fromUpstream, forwardingPlan, newAnwser, fromUpstream.constraints, fromUpstream.unifiers));
    }

    @Override
    public Either<Request, Response> receiveExhausted(final Request fromUpstream, final Response.Exhausted fromDownstream, final ResponseProducer responseProducer) {
        responseProducer.downstreamExhausted(fromDownstream.sourceRequest());
        Plan responsePlan = getResponsePlan(fromUpstream);
        return Either.second(new Response.Exhausted(fromUpstream, responsePlan));
    }

    ResponseProducer createResponseProducer(final Request request) {
        ResponseProducer responseProducer = new ResponseProducer();
        Plan nextStep = request.plan().addStep(whenActor).toNextStep();
        Request toDownstream = new Request(
                nextStep,
                request.partialAnswers,
                request.constraints,
                request.unifiers
        );
        responseProducer.addAvailableDownstream(toDownstream);
        return responseProducer;
    }

    private Plan getResponsePlan(final Request fromUpstream) {
        return fromUpstream.plan().endStepCompleted();
    }

    private Plan forwardingPlan(final Response.Answer fromDownstream) {
        return fromDownstream.plan.endStepCompleted();
    }
}
