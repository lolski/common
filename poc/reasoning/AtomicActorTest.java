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
    public void singleActor() throws InterruptedException {
        LinkedBlockingQueue<Long> responses = new LinkedBlockingQueue<>();
        EventLoopSingleThreaded eventLoop = new EventLoopSingleThreaded(NamedThreadFactory.create(AtomicActor.class, "main"));
        Actor<ActorRoot> rootActor = Actor.root(eventLoop, ActorRoot::new);
        Actor<AtomicActor> atomic = rootActor.ask(root -> root.<AtomicActor>createActor((self) -> new AtomicActor(self, "A", 0L, 2L, responses))).await();

        long startTime = System.currentTimeMillis();
        int n = 5;
        for (int i = 0; i < n; i++) {
            atomic.tell(actor ->
                    actor.receiveRequest(
                            new Request(new Routing(Arrays.asList(), Arrays.asList(atomic)), Arrays.asList(), Arrays.asList(), Arrays.asList())
                    )
            );
        }
        System.out.println("Time : " + (System.currentTimeMillis() - startTime));
        Thread.sleep(20);
    }


    @Test
    public void basic() throws InterruptedException {
        LinkedBlockingQueue<Long> responses = new LinkedBlockingQueue<>();
        EventLoopSingleThreaded eventLoop = new EventLoopSingleThreaded(NamedThreadFactory.create(AtomicActor.class, "main"));
        Actor<ActorRoot> rootActor = Actor.root(eventLoop, ActorRoot::new);
        Actor<AtomicActor> atomic = rootActor.ask(root -> root.<AtomicActor>createActor((self) -> new AtomicActor(self, "B", 2L, 2L, responses))).await();
        Actor<AtomicActor> subAtomic = rootActor.ask(root -> root.<AtomicActor>createActor((self) -> new AtomicActor(self,"A", 20L, 2L, null))).await();

        long startTime = System.currentTimeMillis();
        int n = 4;
        for (int i = 0; i < n; i++) {
            atomic.tell(actor ->
                    actor.receiveRequest(
                            new Request(new Routing(Arrays.asList(), Arrays.asList(subAtomic, atomic)), Arrays.asList(), Arrays.asList(), Arrays.asList())
                    )
            );
        }
        responses.take();
        responses.take();
        responses.take();
        responses.take();
        System.out.println("Time : " + (System.currentTimeMillis() - startTime));
        assertTrue(responses.isEmpty());
    }

    @Test
    public void shallowRerequest() throws InterruptedException {
        LinkedBlockingQueue<Long> responses = new LinkedBlockingQueue<>();
        EventLoopSingleThreaded eventLoop = new EventLoopSingleThreaded(NamedThreadFactory.create(AtomicActor.class, "main"));
        Actor<ActorRoot> rootActor = Actor.root(eventLoop, ActorRoot::new);
        Actor<AtomicActor> atomic = rootActor.ask(root -> root.<AtomicActor>createActor((self) -> new AtomicActor(self, "Downstream", 2L, 2L, responses))).await();
        Actor<AtomicActor> subAtomic = rootActor.ask(root -> root.<AtomicActor>createActor((self) -> new AtomicActor(self, "Downstream", 20L, 2L, null))).await();

        long startTime = System.currentTimeMillis();
        int n = 4;
        for (int i = 0; i < n; i++) {
            atomic.tell(actor ->
                    actor.receiveRequest(
                            new Request(new Routing(Arrays.asList(), Arrays.asList(subAtomic, atomic)), Arrays.asList(), Arrays.asList(), Arrays.asList())
                    )
            );
        }
        for (int i = 0; i < n; i++) {
            responses.take();
        }
        System.out.println("Time : " + (System.currentTimeMillis() - startTime));
        assertTrue(responses.isEmpty());
    }

    @Test
    public void deepRerequest() throws InterruptedException {
        LinkedBlockingQueue<Long> responses = new LinkedBlockingQueue<>();
        EventLoopSingleThreaded eventLoop = new EventLoopSingleThreaded(NamedThreadFactory.create(AtomicActor.class, "main"));
        Actor<ActorRoot> rootActor = Actor.root(eventLoop, ActorRoot::new);
        Actor<AtomicActor> atomic = rootActor.ask(root -> root.<AtomicActor>createActor((self) -> new AtomicActor(self, "Downstream", 2L, 10L, responses))).await();
        Actor<AtomicActor> subAtomic = rootActor.ask(root -> root.<AtomicActor>createActor((self) -> new AtomicActor(self, "Downstream", 20L, 10L, responses))).await();
        Actor<AtomicActor> subAtomic1 = rootActor.ask(root -> root.<AtomicActor>createActor((self) -> new AtomicActor(self, "Downstream", 200L, 10L, responses))).await();
        Actor<AtomicActor> subAtomic2 = rootActor.ask(root -> root.<AtomicActor>createActor((self) -> new AtomicActor(self, "Downstream", 2000L, 10L, responses))).await();
        Actor<AtomicActor> subAtomic3 = rootActor.ask(root -> root.<AtomicActor>createActor((self) -> new AtomicActor(self, "Downstream", 20000L, 10L, responses))).await();

        long startTime = System.currentTimeMillis();
        int n = 1000;
        for (int i = 0; i < n; i++) {
            atomic.tell(actor ->
                    actor.receiveRequest(
                            new Request(new Routing(Arrays.asList(), Arrays.asList(subAtomic3, subAtomic2, subAtomic1, subAtomic, atomic)), Arrays.asList(), Arrays.asList(), Arrays.asList())
                    )
            );
        }
        for (int i = 0; i < n; i++) {
            responses.take();
        }
        System.out.println("Time : " + (System.currentTimeMillis() - startTime));
        assertTrue(responses.isEmpty());
    }
}
