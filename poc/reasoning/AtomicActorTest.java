package grakn.common.poc.reasoning;

import grakn.common.concurrent.NamedThreadFactory;
import grakn.common.concurrent.actor.Actor;
import grakn.common.concurrent.actor.ActorRoot;
import grakn.common.concurrent.actor.eventloop.EventLoopSingleThreaded;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.LinkedBlockingQueue;

import static junit.framework.TestCase.assertTrue;

public class AtomicActorTest {
    @Test
    public void basic() throws InterruptedException {
        LinkedBlockingQueue<Long> answers = new LinkedBlockingQueue<>();
        EventLoopSingleThreaded eventLoop = new EventLoopSingleThreaded(NamedThreadFactory.create(AtomicActor.class, "main"));
        Actor<ActorRoot> rootActor = Actor.root(eventLoop, ActorRoot::new);
        Actor<AtomicActor> atomic = rootActor.ask(root -> root.<AtomicActor>createActor((self) -> new AtomicActor(self, 2L, 2L, answers))).await();
        Actor<AtomicActor> subAtomic = rootActor.ask(root -> root.<AtomicActor>createActor((self) -> new AtomicActor(self, 20L, 2L, null))).await();

        long startTime = System.currentTimeMillis();
        int n = 4;
        for (int i = 0; i < n; i++) {
            atomic.tell(actor ->
                    actor.receiveRequest(
                            new Request(Arrays.asList(), Arrays.asList(subAtomic, atomic), Arrays.asList(), Arrays.asList(), Arrays.asList())
                    )
            );
            answers.take();
        }
        System.out.println("Time : " + (System.currentTimeMillis() - startTime));
        assertTrue(answers.isEmpty());
    }

    @Test
    public void shallowRerequest() throws InterruptedException {
        LinkedBlockingQueue<Long> answers = new LinkedBlockingQueue<>();
        EventLoopSingleThreaded eventLoop = new EventLoopSingleThreaded(NamedThreadFactory.create(AtomicActor.class, "main"));
        Actor<ActorRoot> rootActor = Actor.root(eventLoop, ActorRoot::new);
        Actor<AtomicActor> atomic = rootActor.ask(root -> root.<AtomicActor>createActor((self) -> new AtomicActor(self, 2L, 2L, answers))).await();
        Actor<AtomicActor> subAtomic = rootActor.ask(root -> root.<AtomicActor>createActor((self) -> new AtomicActor(self, 20L, 2L, null))).await();

        long startTime = System.currentTimeMillis();
        int n = 4;
        for (int i = 0; i < n; i++) {
            atomic.tell(actor ->
                    actor.receiveRequest(
                            new Request(Arrays.asList(), Arrays.asList(subAtomic, atomic), Arrays.asList(), Arrays.asList(), Arrays.asList())
                    )
            );
        }
        for (int i = 0; i < n; i++) {
            answers.take();
        }
        System.out.println("Time : " + (System.currentTimeMillis() - startTime));
        assertTrue(answers.isEmpty());
    }

    @Test
    public void deepRerequest() throws InterruptedException {
        LinkedBlockingQueue<Long> answers = new LinkedBlockingQueue<>();
        EventLoopSingleThreaded eventLoop = new EventLoopSingleThreaded(NamedThreadFactory.create(AtomicActor.class, "main"));
        Actor<ActorRoot> rootActor = Actor.root(eventLoop, ActorRoot::new);
        Actor<AtomicActor> atomic = rootActor.ask(root -> root.<AtomicActor>createActor((self) -> new AtomicActor(self, 2L, 10L, answers))).await();
        Actor<AtomicActor> subAtomic = rootActor.ask(root -> root.<AtomicActor>createActor((self) -> new AtomicActor(self, 20L, 10L, answers))).await();
        Actor<AtomicActor> subAtomic1 = rootActor.ask(root -> root.<AtomicActor>createActor((self) -> new AtomicActor(self, 200L, 10L, answers))).await();
        Actor<AtomicActor> subAtomic2 = rootActor.ask(root -> root.<AtomicActor>createActor((self) -> new AtomicActor(self, 2000L, 10L, answers))).await();
        Actor<AtomicActor> subAtomic3 = rootActor.ask(root -> root.<AtomicActor>createActor((self) -> new AtomicActor(self, 20000L, 10L, answers))).await();

        long startTime = System.currentTimeMillis();
        int n = 1000;
        for (int i = 0; i < n; i++) {
            atomic.tell(actor ->
                    actor.receiveRequest(
                            new Request(Arrays.asList(), Arrays.asList(subAtomic3, subAtomic2, subAtomic1, subAtomic, atomic), Arrays.asList(), Arrays.asList(), Arrays.asList())
                    )
            );
        }
        for (int i = 0; i < n; i++) {
            answers.take();
        }
        System.out.println("Time : " + (System.currentTimeMillis() - startTime));
        assertTrue(answers.isEmpty());
    }
}
