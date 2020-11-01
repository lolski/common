package grakn.common.poc.reasoning;

import grakn.common.concurrent.NamedThreadFactory;
import grakn.common.concurrent.actor.Actor;
import grakn.common.concurrent.actor.ActorRoot;
import grakn.common.concurrent.actor.eventloop.EventLoopSingleThreaded;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class ActorManager {
    Logger LOG = LoggerFactory.getLogger(ActorManager.class.getSimpleName());

    private Map<Long, Actor<AtomicActor>> atomicActors;
    private Actor<ActorRoot> rootActor;
    private LinkedBlockingQueue<Long> responses;

    public ActorManager() {
        atomicActors = new HashMap<>();
        responses = new LinkedBlockingQueue<>();
        EventLoopSingleThreaded eventLoop = new EventLoopSingleThreaded(NamedThreadFactory.create(AtomicActor.class, "main"));
        rootActor = Actor.root(eventLoop, ActorRoot::new);
    }

    public Actor<ConjunctiveActor> createRootConjunctiveActor(List<Long> conjunction, long traversalSize) throws InterruptedException {
        Actor<ConjunctiveActor> conjunctive = rootActor.ask(root -> root.<ConjunctiveActor>createActor(self -> {
            try {
                return new ConjunctiveActor(self, this, conjunction, traversalSize, responses);
            } catch (InterruptedException e) {
                LOG.error("Failed to create conjunctive actor: " + conjunction);
                throw new RuntimeException(e);
            }
        })).await();
        return conjunctive;
    }

    public Actor<ConjunctiveActor> createConjunctiveActor(List<Long> conjunction, long traversalSize) throws InterruptedException {
        Actor<ConjunctiveActor> conjunctive = rootActor.ask(root -> root.<ConjunctiveActor>createActor(self -> {
            try {
                return new ConjunctiveActor(self, this, conjunction, traversalSize, null);
            } catch (InterruptedException e) {
                LOG.error("Failed to create conjunctive actor: " + conjunction);
                throw new RuntimeException(e);
            }
        })).await();
        return conjunctive;
    }

    public Actor<AtomicActor> createAtomicActor(Long traversalPattern, Long traversalSize, List<List<Long>> rules) throws InterruptedException {
        Actor<AtomicActor> atomic = rootActor.ask(root -> root.<AtomicActor>createActor((self) -> {
            try {
                return new AtomicActor(self, this, traversalPattern, traversalSize, rules);
            } catch (InterruptedException e) {
                LOG.error("Failed to create atomic actor: " + traversalPattern);
                throw new RuntimeException(e);
            }
        })).await();
        assert !atomicActors.containsKey(traversalPattern);
        atomicActors.put(traversalPattern, atomic);
        return atomic;
    }

    public Actor<RuleActor> createRuleActor(final List<Long> whenPattern, final long traversalSize) throws InterruptedException {
        Actor<RuleActor> actor = rootActor.ask(root -> root.<RuleActor>createActor(self -> {
            try {
                return new RuleActor(self, this, whenPattern, traversalSize);
            } catch (InterruptedException e) {
                LOG.error("Failed to create rule actor: " + whenPattern);
                throw new RuntimeException(e);
            }
        })).await();
        return actor;
    }

    public Actor<AtomicActor> getAtomicActor(Long traversalPattern) {
        return atomicActors.get(traversalPattern);
    }

    public Long takeAnswer() throws InterruptedException {
        return responses.take();
    }

    public boolean hasAnswer() {
        return !responses.isEmpty();
    }

}
