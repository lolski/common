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
                            new Request(new Path(Arrays.asList(atomic)), Arrays.asList(), Arrays.asList(), Arrays.asList())
                    )
            );
            responses.take();
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
                            new Request(new Path(Arrays.asList(atomic, subAtomic)), Arrays.asList(), Arrays.asList(), Arrays.asList())
                    )
            );
        }
        Thread.sleep(1000);
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
        Actor<AtomicActor> atomic = rootActor.ask(root -> root.<AtomicActor>createActor((self) -> new AtomicActor(self, "3", 2L, 1L, responses))).await();
        Actor<AtomicActor> subAtomic1 = rootActor.ask(root -> root.<AtomicActor>createActor((self) -> new AtomicActor(self, "2", 20L, 1L, null))).await();
        Actor<AtomicActor> subAtomic2 = rootActor.ask(root -> root.<AtomicActor>createActor((self) -> new AtomicActor(self, "1", 200L, 1L, null))).await();

        long startTime = System.currentTimeMillis();
        int n = 5;
        for (int i = 0; i < n; i++) {
            atomic.tell(actor ->
                    actor.receiveRequest(
                            new Request(new Path(Arrays.asList(atomic, subAtomic1, subAtomic2)), Arrays.asList(), Arrays.asList(), Arrays.asList())
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
        Actor<AtomicActor> atomic = rootActor.ask(root -> root.<AtomicActor>createActor((self) -> new AtomicActor(self, "5", 2L, 10L, responses))).await();
        Actor<AtomicActor> subAtomic = rootActor.ask(root -> root.<AtomicActor>createActor((self) -> new AtomicActor(self, "4", 20L, 10L, responses))).await();
        Actor<AtomicActor> subAtomic1 = rootActor.ask(root -> root.<AtomicActor>createActor((self) -> new AtomicActor(self, "3", 200L, 10L, responses))).await();
        Actor<AtomicActor> subAtomic2 = rootActor.ask(root -> root.<AtomicActor>createActor((self) -> new AtomicActor(self, "2", 2000L, 10L, responses))).await();
        Actor<AtomicActor> subAtomic3 = rootActor.ask(root -> root.<AtomicActor>createActor((self) -> new AtomicActor(self, "1", 20000L, 10L, responses))).await();

        long startTime = System.currentTimeMillis();
        int n = 10000;
        for (int i = 0; i < n; i++) {
            atomic.tell(actor ->
                    actor.receiveRequest(
                            new Request(new Path(Arrays.asList(atomic, subAtomic, subAtomic1, subAtomic2, subAtomic3)), Arrays.asList(), Arrays.asList(), Arrays.asList())
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
