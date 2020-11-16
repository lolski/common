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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

// TODO settle on a name
public class Atomic extends ExecutionActor<Atomic> {
    private static final Logger LOG = LoggerFactory.getLogger(Atomic.class);

    private final Long traversalPattern;
    private final long traversalSize;
    private final List<List<Long>> rules;
    private final List<Actor<Rule>> ruleActors;
    private final Set<RuleTrigger> triggered;

    public Atomic(final Actor<Atomic> self, final Long traversalPattern, final List<List<Long>> rules, final long traversalSize) {
        super(self, Atomic.class.getSimpleName() + "(pattern: " + traversalPattern + ")");
        this.traversalPattern = traversalPattern;
        this.traversalSize = traversalSize;
        this.rules = rules;
        ruleActors = new ArrayList<>();
        triggered = new HashSet<>();
    }

    @Override
    public Either<Request, Response> receiveRequest(final Request fromUpstream, final ResponseProducer responseProducer) {
        return produce(fromUpstream, responseProducer);
    }

    @Override
    public Either<Request, Response> receiveAnswer(final Request fromUpstream, final Response.Answer fromDownstream,
                                                   ResponseProducer responseProducer) {
        // TODO may combine with partial answers from the fromUpstream message
        LOG.debug(this.name + ": hasProduced: " + fromDownstream.partialAnswer());
        if (!responseProducer.hasProduced(fromDownstream.partialAnswer())) {
            responseProducer.recordProduced(fromDownstream.partialAnswer());
            return Either.second(new Response.Answer(fromUpstream, fromDownstream.partialAnswer(),
                    fromUpstream.constraints(), fromUpstream.unifiers()));
        } else {
            return produce(fromUpstream, responseProducer);
        }
    }

    @Override
    public Either<Request, Response> receiveExhausted(final Request fromUpstream, final Response.Exhausted fromDownstream, final ResponseProducer responseProducer) {
        responseProducer.removeReadyDownstream(fromDownstream.sourceRequest());
        return produce(fromUpstream, responseProducer);
    }

    @Override
    protected ResponseProducer createResponseProducer(final Request request) {
        Iterator<List<Long>> traversal = (new MockTransaction(traversalSize, traversalPattern, 1)).query(request.partialAnswer());
        ResponseProducer responseProducer = new ResponseProducer(traversal);

        RuleTrigger trigger = new RuleTrigger(request.partialAnswer(), request.constraints());
        if (!triggered.contains(trigger)) {
            registerDownstreamRules(responseProducer, request.path(), request.partialAnswer(), request.constraints(), request.unifiers());
            triggered.add(trigger);
        }
        return responseProducer;
    }

    @Override
    protected void initialiseDownstreamActors(Registry registry) {
        for (List<Long> rule : rules) {
            Actor<Rule> ruleActor = registry.registerRule(rule, pattern -> Actor.create(self().eventLoopGroup(), actor -> new Rule(actor, pattern, 1L, 0L)));
            ruleActors.add(ruleActor);
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

    private void registerDownstreamRules(final ResponseProducer responseProducer, final Request.Path path, final List<Long> partialAnswers,
                                         final List<Object> constraints, final List<Object> unifiers) {
        for (Actor<Rule> ruleActor : ruleActors) {
            Request toDownstream = new Request(path.append(ruleActor), partialAnswers, constraints, unifiers);
            responseProducer.addReadyDownstream(toDownstream);
        }
    }

    private static class RuleTrigger {
        private final List<Long> partialAnswer;
        private final List<Object> constraints;

        public RuleTrigger(List<Long> partialAnswer, List<Object> constraints) {
            this.partialAnswer = partialAnswer;
            this.constraints = constraints;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RuleTrigger that = (RuleTrigger) o;
            return Objects.equals(partialAnswer, that.partialAnswer) &&
                    Objects.equals(constraints, that.constraints);
        }

        @Override
        public int hashCode() {
            return Objects.hash(partialAnswer, constraints);
        }
    }

}

