package grakn.common.poc.reasoning;

import grakn.common.collection.Either;
import grakn.common.concurrent.actor.Actor;
import grakn.common.poc.reasoning.mock.MockTransaction;
import grakn.common.poc.reasoning.framework.Derivations;
import grakn.common.poc.reasoning.framework.ExecutionActor;
import grakn.common.poc.reasoning.framework.Registry;
import grakn.common.poc.reasoning.framework.Request;
import grakn.common.poc.reasoning.framework.Response;
import grakn.common.poc.reasoning.framework.ResponseProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static grakn.common.collection.Collections.map;
import static grakn.common.collection.Collections.pair;

public class Concludable extends ExecutionActor<Concludable> {
    private static final Logger LOG = LoggerFactory.getLogger(Concludable.class);

    private final Long traversalPattern;
    private final long traversalSize;
    private final List<List<Long>> rules;
    private final Map<Actor<Rule>, List<Long>> ruleActorSources;
    private final Set<RuleTrigger> triggered;
    private Actor<DerivationRecorder> recorder;

    public Concludable(Actor<Concludable> self, Long traversalPattern, List<List<Long>> rules, long traversalSize) {
        super(self, Concludable.class.getSimpleName() + "(pattern: " + traversalPattern + ")");
        this.traversalPattern = traversalPattern;
        this.traversalSize = traversalSize;
        this.rules = rules;
        ruleActorSources = new HashMap<>();
        triggered = new HashSet<>();
    }

    @Override
    public Either<Request, Response> receiveRequest(Request fromUpstream, ResponseProducer responseProducer) {
        return produceMessage(fromUpstream, responseProducer);
    }

    @Override
    public Either<Request, Response> receiveAnswer(Request fromUpstream, Response.Answer fromDownstream,
                                                   ResponseProducer responseProducer) {
        Actor<? extends ExecutionActor<?>> ruleSender = fromDownstream.sourceRequest().receiver();

        List<Long> rulePattern = ruleActorSources.get(ruleSender);

        // TODO may combine with partial answers from the fromUpstream message

        LOG.debug("{}: hasProduced: {}", name, fromDownstream.partialAnswer());
        if (!responseProducer.hasProduced(fromDownstream.partialAnswer())) {
            responseProducer.recordProduced(fromDownstream.partialAnswer());

            // update partial explanation provided from upstream to carry explanations sideways
            Derivations derivations = new Derivations(map(pair(fromDownstream.sourceRequest().receiver(), fromDownstream)));

            return Either.second(new Response.Answer(fromUpstream, fromDownstream.partialAnswer(), fromUpstream.unifiers(),
                    traversalPattern.toString(), derivations));
        } else {
            Derivations derivations = new Derivations(map(pair(fromDownstream.sourceRequest().receiver(), fromDownstream)));
            Response.Answer deduplicated = new Response.Answer(fromUpstream, fromDownstream.partialAnswer(), fromUpstream.unifiers(),
                    traversalPattern.toString(), derivations);
            recorder.tell(actor -> actor.recordTree(deduplicated));

            return produceMessage(fromUpstream, responseProducer);
        }
    }

    @Override
    public Either<Request, Response> receiveExhausted(Request fromUpstream, Response.Exhausted fromDownstream, ResponseProducer responseProducer) {
        responseProducer.removeDownstreamProducer(fromDownstream.sourceRequest());
        return produceMessage(fromUpstream, responseProducer);
    }

    @Override
    protected ResponseProducer createResponseProducer(Request request) {
        Iterator<List<Long>> traversal = (new MockTransaction(traversalSize, traversalPattern, 1)).query(request.partialAnswer());
        ResponseProducer responseProducer = new ResponseProducer(traversal);

        RuleTrigger trigger = new RuleTrigger(request.partialAnswer());
        if (!triggered.contains(trigger)) {
            registerDownstreamRules(responseProducer, request.path(), request.partialAnswer(), request.unifiers());
            triggered.add(trigger);
        }
        return responseProducer;
    }

    @Override
    protected void initialiseDownstreamActors(Registry registry) {
        recorder = registry.explanationRecorder();
        for (List<Long> rule : rules) {
            Actor<Rule> ruleActor = registry.registerRule(rule, pattern -> Actor.create(self().eventLoopGroup(), actor -> new Rule(actor, pattern, 1L, 0L)));
            ruleActorSources.put(ruleActor, rule);
        }
    }

    private Either<Request, Response> produceMessage(Request fromUpstream, ResponseProducer responseProducer) {
        while (responseProducer.hasTraversalProducer()) {
            List<Long> answer = responseProducer.traversalProducer().next();
            LOG.debug("{}: hasProduced: {}", name, answer);
            if (!responseProducer.hasProduced(answer)) {
                responseProducer.recordProduced(answer);
                return Either.second(new Response.Answer(fromUpstream, answer,
                        fromUpstream.unifiers(), traversalPattern.toString(), new Derivations(map())));
            }
        }

        if (responseProducer.hasDownstreamProducer()) {
            return Either.first(responseProducer.nextDownstreamProducer());
        } else {
            return Either.second(new Response.Exhausted(fromUpstream));
        }
    }

    private void registerDownstreamRules(ResponseProducer responseProducer, Request.Path path, List<Long> partialAnswers,
                                         List<Object> unifiers) {
        for (Actor<Rule> ruleActor : ruleActorSources.keySet()) {
            Request toDownstream = new Request(path.append(ruleActor), partialAnswers, unifiers, Derivations.EMPTY);
            responseProducer.addDownstreamProducer(toDownstream);
        }
    }

    @Override
    protected void exception(Exception e) {
        LOG.error("Actor exception", e);
        // TODO, once integrated into the larger flow of executing queries, kill the actors and report and exception to root
    }

    private static class RuleTrigger {
        private final List<Long> partialAnswer;

        public RuleTrigger(List<Long> partialAnswer) {
            this.partialAnswer = partialAnswer;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RuleTrigger that = (RuleTrigger) o;
            return Objects.equals(partialAnswer, that.partialAnswer);
        }

        @Override
        public int hashCode() {
            return Objects.hash(partialAnswer);
        }
    }

}

