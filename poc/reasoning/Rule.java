package grakn.common.poc.reasoning;

import grakn.common.collection.Either;
import grakn.common.concurrent.actor.Actor;

import java.util.List;

public class Rule extends ExecutionActor<Rule> {
    private final List<Long> when;
    private final Long whenTraversalSize;
    private Actor<Conjunction> whenActor = null;

    public Rule(final Actor<Rule> self, final List<Long> when,
                final Long whenTraversalSize) {
        super(self, Rule.class.getSimpleName() + "(pattern:" + when + ")");
        this.when = when;
        this.whenTraversalSize = whenTraversalSize;
    }

    @Override
    public Either<Request, Response> receiveRequest(final Request fromUpstream, final ResponseProducer responseProducer) {
        assert fromUpstream.plan().atEnd() : "A rule that receives a fromUpstream must be at the end of the plan";

        Plan responsePlan = respondingPlan(fromUpstream);

        if (!responseProducer.downstreamsExhausted()) {
            return Either.first(responseProducer.getAvailableDownstream());
        } else {
            return Either.second(new Response.Exhausted(fromUpstream, responsePlan));
        }
    }

    @Override
    public Either<Request, Response> receiveAnswer(final Request fromUpstream, final Response.Answer fromDownstream, final ResponseProducer responseProducer) {
        Plan forwardingPlan = forwardingPlan(fromDownstream);

        List<Long> newAnwser = fromDownstream.partialAnswer;

        // TODO unify and materialise

        return Either.second(
                new Response.Answer(fromUpstream, forwardingPlan, newAnwser, fromUpstream.constraints, fromUpstream.unifiers));
    }

    @Override
    public Either<Request, Response> receiveExhausted(final Request fromUpstream, final Response.Exhausted fromDownstream, final ResponseProducer responseProducer) {
        responseProducer.downstreamExhausted(fromDownstream.sourceRequest());
        Plan responsePlan = respondingPlan(fromUpstream);
        return Either.second(new Response.Exhausted(fromUpstream, responsePlan));
    }

    @Override
    ResponseProducer createResponseProducer(final Request request) {
        ResponseProducer responseProducer = new ResponseProducer();
        Plan nextStep = request.plan().addStep(whenActor).toNextStep();
        Request toDownstream = new Request(
                nextStep,
                request.partialAnswer,
                request.constraints,
                request.unifiers
        );
        responseProducer.addAvailableDownstream(toDownstream);
        return responseProducer;
    }

    @Override
    void initialiseDownstreamActors(ActorRegistry actorRegistry) {
        whenActor = child((newActor) -> new Conjunction(newActor, when, whenTraversalSize));
    }

    private Plan respondingPlan(final Request fromUpstream) {
        return fromUpstream.plan().endStepCompleted();
    }

    private Plan forwardingPlan(final Response.Answer fromDownstream) {
        return fromDownstream.plan.endStepCompleted();
    }
}
