package grakn.common.poc.reasoning;

import grakn.common.concurrent.NamedThreadFactory;
import grakn.common.concurrent.actor.Actor;
import grakn.common.concurrent.actor.ActorRoot;
import grakn.common.concurrent.actor.eventloop.EventLoopSingleThreaded;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class ActorManager {
    private Map<Long, Actor<AtomicActor>> atomicActors;
    private Actor<ActorRoot> rootActor;
    private LinkedBlockingQueue<Long> responses;

    public ActorManager() {
        atomicActors = new HashMap<>();
        responses = new LinkedBlockingQueue<>();
        EventLoopSingleThreaded eventLoop = new EventLoopSingleThreaded(NamedThreadFactory.create(AtomicActor.class, "main"));
        rootActor = Actor.root(eventLoop, ActorRoot::new);
    }

    public Actor<AtomicActor> createAtomicActor(String name, Long traversalPattern, Long traversalAnswerSize, boolean outputAnswers) throws InterruptedException {
        LinkedBlockingQueue<Long> responses = outputAnswers ? this.responses : null;
        Actor<AtomicActor> atomic = rootActor.ask(root -> root.<AtomicActor>createActor((self) -> new AtomicActor(self, name, traversalPattern, traversalAnswerSize, responses))).await();
        atomicActors.put(traversalPattern, atomic);
        return atomic;
    }

    public Long takeAnswer() throws InterruptedException {
        return responses.take();
    }

    public boolean hasAnswer() {
        return !responses.isEmpty();
    }
}
