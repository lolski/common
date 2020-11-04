package grakn.common.poc.reasoning;

import grakn.common.concurrent.NamedThreadFactory;
import grakn.common.concurrent.actor.Actor;
import grakn.common.concurrent.actor.ActorRoot;
import grakn.common.concurrent.actor.eventloop.EventLoopSingleThreaded;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.LinkedBlockingQueue;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

public class ReasoningTest {
    @Test
    public void singleAtomicActor() throws InterruptedException {
        ActorRegistry actorRegistry = new ActorRegistry();

        LinkedBlockingQueue<Long> responses = new LinkedBlockingQueue<>();
        EventLoopSingleThreaded eventLoop = new EventLoopSingleThreaded(NamedThreadFactory.create(ReasoningTest.class, "main"));
        Actor<ActorRoot> rootActor = Actor.root(eventLoop, ActorRoot::new);

        // create atomic actors first to control answer size
        actorRegistry.registerAtomic(0L, pattern ->
                rootActor.ask(actor ->
                        actor.<AtomicActor>createActor(self -> new AtomicActor(self, actorRegistry, pattern, 5L, Arrays.asList()))
                ).awaitUnchecked()
        );
        Actor<ConjunctiveActor> conjunctive = rootActor.ask(actor ->
                actor.<ConjunctiveActor>createActor(self -> new ConjunctiveActor(self, actorRegistry, Arrays.asList(0L), 5L, responses))
        ).awaitUnchecked();


        long startTime = System.currentTimeMillis();
        long n = 5L + 5L + 1; //total number of traversal answers
        for (int i = 0; i < n; i++) {
            conjunctive.tell(actor ->
                    actor.receiveRequest(
                            new Request(new Plan(Arrays.asList(conjunctive)).toNextStep(), Arrays.asList(), Arrays.asList(), Arrays.asList())
                    )
            );
            Long answer = responses.take();
            System.out.println(answer);
            if (i < n - 1) {
                assertTrue(answer != -1);
            } else {
                assertTrue(answer == -1);
            }
        }
        System.out.println("Time : " + (System.currentTimeMillis() - startTime));
        Thread.sleep(20);
    }


    @Test
    public void doubleAtomicActor() throws InterruptedException {
        ActorRegistry actorRegistry = new ActorRegistry();

        LinkedBlockingQueue<Long> responses = new LinkedBlockingQueue<>();
        EventLoopSingleThreaded eventLoop = new EventLoopSingleThreaded(NamedThreadFactory.create(ReasoningTest.class, "main"));
        Actor<ActorRoot> rootActor = Actor.root(eventLoop, ActorRoot::new);

        // create atomic actors first to control answer size
        actorRegistry.registerAtomic(2L, pattern ->
                rootActor.ask(actor ->
                        actor.<AtomicActor>createActor(self -> new AtomicActor(self, actorRegistry, pattern, 2L, Arrays.asList()))
                ).awaitUnchecked()
        );
        actorRegistry.registerAtomic(20L, pattern ->
                rootActor.ask(actor ->
                        actor.<AtomicActor>createActor(self -> new AtomicActor(self, actorRegistry, pattern, 2L, Arrays.asList()))
                ).awaitUnchecked()
        );
        Actor<ConjunctiveActor> conjunctive = rootActor.ask(actor ->
                actor.<ConjunctiveActor>createActor(self -> new ConjunctiveActor(self, actorRegistry, Arrays.asList(20L, 2L), 0L, responses))
        ).awaitUnchecked();


        long startTime = System.currentTimeMillis();
        long n = 0L + (2L * 2L) + 1; //total number of traversal answers, plus one expected DONE (-1 answer)
        for (int i = 0; i < n; i++) {
            conjunctive.tell(actor ->
                    actor.receiveRequest(
                            new Request(new Plan(Arrays.asList(conjunctive)).toNextStep(), Arrays.asList(), Arrays.asList(), Arrays.asList())
                    )
            );
        }

        for (int i = 0; i < n - 1; i++) {
            Long answer = responses.take();
            assertTrue(answer != -1);
        }
        assertEquals(responses.take().longValue(), -1L);
//        Thread.sleep(1000); // enable for debugging to ensure equivalent debug vs normal execution
        System.out.println("Time : " + (System.currentTimeMillis() - startTime));
        assertTrue(responses.isEmpty());
    }


    @Test
    public void filteringAtomicActor() throws InterruptedException {
        ActorRegistry actorRegistry = new ActorRegistry();

        LinkedBlockingQueue<Long> responses = new LinkedBlockingQueue<>();
        EventLoopSingleThreaded eventLoop = new EventLoopSingleThreaded(NamedThreadFactory.create(ReasoningTest.class, "main"));
        Actor<ActorRoot> rootActor = Actor.root(eventLoop, ActorRoot::new);

        // create atomic actors first to control answer size
        actorRegistry.registerAtomic(2L, pattern ->
                rootActor.ask(actor ->
                        actor.<AtomicActor>createActor(self -> new AtomicActor(self, actorRegistry, pattern, 2L, Arrays.asList()))
                ).awaitUnchecked()
        );
        actorRegistry.registerAtomic(20L, pattern ->
                rootActor.ask(actor ->
                        actor.<AtomicActor>createActor(self -> new AtomicActor(self, actorRegistry, pattern, 0L, Arrays.asList()))
                ).awaitUnchecked()
        );
        Actor<ConjunctiveActor> conjunctive = rootActor.ask(actor ->
                actor.<ConjunctiveActor>createActor(self -> new ConjunctiveActor(self, actorRegistry, Arrays.asList(20L, 2L), 0L, responses))
        ).awaitUnchecked();


        long startTime = System.currentTimeMillis();
        long n = 0L + (0L * 0L) + 1; // total number of traversal answers, plus one expected DONE (-1 answer)
        for (int i = 0; i < n; i++) {
            conjunctive.tell(actor ->
                    actor.receiveRequest(
                            new Request(new Plan(Arrays.asList(conjunctive)).toNextStep(), Arrays.asList(), Arrays.asList(), Arrays.asList())
                    )
            );
        }

        for (int i = 0; i < n - 1; i++) {
            Long answer = responses.take();
            assertTrue(answer != -1);
        }
        assertEquals(responses.take().longValue(), -1L);
//        Thread.sleep(1000); // enable for debugging to ensure equivalent debug vs normal execution
        System.out.println("Time : " + (System.currentTimeMillis() - startTime));
        assertTrue(responses.isEmpty());
    }

    @Test
    public void simpleRule() throws InterruptedException {
        ActorRegistry actorRegistry = new ActorRegistry();

        LinkedBlockingQueue<Long> responses = new LinkedBlockingQueue<>();
        EventLoopSingleThreaded eventLoop = new EventLoopSingleThreaded(NamedThreadFactory.create(ReasoningTest.class, "main"));
        Actor<ActorRoot> rootActor = Actor.root(eventLoop, ActorRoot::new);

        // create atomic actors first to control answer size
        actorRegistry.registerAtomic(-2L, pattern ->
                rootActor.ask(actor ->
                        actor.<AtomicActor>createActor(self -> new AtomicActor(self, actorRegistry, pattern, 1L, Arrays.asList()))
                ).awaitUnchecked()
        );
        actorRegistry.registerAtomic(2L, pattern ->
                rootActor.ask(actor ->
                        actor.<AtomicActor>createActor(self -> new AtomicActor(self, actorRegistry, pattern, 1L, Arrays.asList(Arrays.asList(-2L))))
                ).awaitUnchecked()
        );
        Actor<ConjunctiveActor> conjunctive = rootActor.ask(actor ->
                actor.<ConjunctiveActor>createActor(self -> new ConjunctiveActor(self, actorRegistry, Arrays.asList(2L), 0L, responses))
        ).awaitUnchecked();

        long startTime = System.currentTimeMillis();
        long n = 0L + (1L) + (1L) + (1L) + 1; //total number of traversal answers, plus one expected DONE (-1 answer)
        for (int i = 0; i < n; i++) {
            conjunctive.tell(actor ->
                    actor.receiveRequest(
                            new Request(new Plan(Arrays.asList(conjunctive)).toNextStep(), Arrays.asList(), Arrays.asList(), Arrays.asList())
                    )
            );
        }

        for (int i = 0; i < n - 1; i++) {
            Long answer = responses.take();
            assertTrue(answer != -1);
        }
        assertEquals(responses.take().longValue(), -1L);
        System.out.println("Time : " + (System.currentTimeMillis() - startTime));
        assertTrue(responses.isEmpty());
    }

    @Test
    public void atomicChainWithRule() throws InterruptedException {
        ActorRegistry actorRegistry = new ActorRegistry();

        LinkedBlockingQueue<Long> responses = new LinkedBlockingQueue<>();
        EventLoopSingleThreaded eventLoop = new EventLoopSingleThreaded(NamedThreadFactory.create(ReasoningTest.class, "main"));
        Actor<ActorRoot> rootActor = Actor.root(eventLoop, ActorRoot::new);

        // create atomic actors first to control answer size
        Actor<AtomicActor> bottomAtomic = actorRegistry.registerAtomic(-2L, pattern ->
                rootActor.ask(actor ->
                        actor.<AtomicActor>createActor(self -> new AtomicActor(self, actorRegistry, pattern, 1L, Arrays.asList()))
                ).awaitUnchecked()
        );
        Actor<AtomicActor> atomicWithRule = actorRegistry.registerAtomic(2L, pattern ->
                rootActor.ask(actor ->
                        actor.<AtomicActor>createActor(self -> new AtomicActor(self, actorRegistry, pattern, 1L, Arrays.asList(Arrays.asList(-2L))))
                ).awaitUnchecked()
        );
        Actor<AtomicActor> atomicWithoutRule = actorRegistry.registerAtomic(20L, pattern ->
                rootActor.ask(actor ->
                        actor.<AtomicActor>createActor(self -> new AtomicActor(self, actorRegistry, pattern, 1L, Arrays.asList()))
                ).awaitUnchecked()
        );
        Actor<ConjunctiveActor> conjunctive = rootActor.ask(actor ->
                actor.<ConjunctiveActor>createActor(self -> new ConjunctiveActor(self, actorRegistry, Arrays.asList(20L, 2L), 0L, responses))
        ).awaitUnchecked();

        long startTime = System.currentTimeMillis();
        long n = 0L + (1*1) + (1*2) + 1; //total number of traversal answers, plus one expected DONE (-1 answer)
        for (int i = 0; i < n; i++) {
            conjunctive.tell(actor ->
                    actor.receiveRequest(
                            new Request(new Plan(Arrays.asList(conjunctive)).toNextStep(), Arrays.asList(), Arrays.asList(), Arrays.asList())
                    )
            );
        }

        for (int i = 0; i < n - 1; i++) {
            Long answer = responses.take();
            System.out.println("---- take(): " + answer);
            assertTrue(answer != -1);
        }
        assertEquals(responses.take().longValue(), -1L);
        System.out.println("Time : " + (System.currentTimeMillis() - startTime));
        assertTrue(responses.isEmpty());
    }


    @Test
    public void shallowRerequest() throws InterruptedException {
        ActorRegistry actorRegistry = new ActorRegistry();

        LinkedBlockingQueue<Long> responses = new LinkedBlockingQueue<>();
        EventLoopSingleThreaded eventLoop = new EventLoopSingleThreaded(NamedThreadFactory.create(ReasoningTest.class, "main"));
        Actor<ActorRoot> rootActor = Actor.root(eventLoop, ActorRoot::new);

        // create atomic actors first to control answer size
        actorRegistry.registerAtomic(2L, pattern ->
                rootActor.ask(actor ->
                        actor.<AtomicActor>createActor(self -> new AtomicActor(self, actorRegistry, pattern, 4L, Arrays.asList()))
                ).awaitUnchecked()
        );
        actorRegistry.registerAtomic(20L, pattern ->
                rootActor.ask(actor ->
                        actor.<AtomicActor>createActor(self -> new AtomicActor(self, actorRegistry, pattern, 4L, Arrays.asList()))
                ).awaitUnchecked()
        );
        actorRegistry.registerAtomic(200L, pattern ->
                rootActor.ask(actor ->
                        actor.<AtomicActor>createActor(self -> new AtomicActor(self, actorRegistry, pattern, 4L, Arrays.asList()))
                ).awaitUnchecked()
        );
        Actor<ConjunctiveActor> conjunctive = rootActor.ask(actor ->
                actor.<ConjunctiveActor>createActor(self -> new ConjunctiveActor(self, actorRegistry, Arrays.asList(200L, 20L, 2L), 0L, responses))
        ).awaitUnchecked();

        long startTime = System.currentTimeMillis();
        long n = 0L + (4L*4L*4L) + 1;
        for (int i = 0; i < n; i++) {
            conjunctive.tell(actor ->
                    actor.receiveRequest(
                            new Request(new Plan(Arrays.asList(conjunctive)).toNextStep(), Arrays.asList(), Arrays.asList(), Arrays.asList())
                    )
            );
        }
        for (int i = 0; i < n - 1; i++) {
            Long answer = responses.take();
            assertTrue(answer != -1);
        }
        assertEquals(responses.take().longValue(), -1L);
        System.out.println("Time : " + (System.currentTimeMillis() - startTime));
        assertTrue(responses.isEmpty());
    }

    @Test
    public void deepRerequest() throws InterruptedException {
        ActorRegistry actorRegistry = new ActorRegistry();

        LinkedBlockingQueue<Long> responses = new LinkedBlockingQueue<>();
        EventLoopSingleThreaded eventLoop = new EventLoopSingleThreaded(NamedThreadFactory.create(ReasoningTest.class, "main"));
        Actor<ActorRoot> rootActor = Actor.root(eventLoop, ActorRoot::new);

        // create atomic actors first to control answer size
        actorRegistry.registerAtomic(2L, pattern ->
                rootActor.ask(actor ->
                        actor.<AtomicActor>createActor(self -> new AtomicActor(self, actorRegistry, pattern, 10L, Arrays.asList()))
                ).awaitUnchecked()
        );
        actorRegistry.registerAtomic(20L, pattern ->
                rootActor.ask(actor ->
                        actor.<AtomicActor>createActor(self -> new AtomicActor(self, actorRegistry, pattern, 10L, Arrays.asList()))
                ).awaitUnchecked()
        );
        actorRegistry.registerAtomic(200L, pattern ->
                rootActor.ask(actor ->
                        actor.<AtomicActor>createActor(self -> new AtomicActor(self, actorRegistry, pattern, 10L, Arrays.asList()))
                ).awaitUnchecked()
        );
        actorRegistry.registerAtomic(2000L, pattern ->
                rootActor.ask(actor ->
                        actor.<AtomicActor>createActor(self -> new AtomicActor(self, actorRegistry, pattern, 10L, Arrays.asList()))
                ).awaitUnchecked()
        );
        actorRegistry.registerAtomic(20000L, pattern ->
                rootActor.ask(actor ->
                        actor.<AtomicActor>createActor(self -> new AtomicActor(self, actorRegistry, pattern, 10L, Arrays.asList()))
                ).awaitUnchecked()
        );
        Actor<ConjunctiveActor> conjunctive = rootActor.ask(actor ->
                actor.<ConjunctiveActor>createActor(self -> new ConjunctiveActor(self, actorRegistry, Arrays.asList(20000L, 2000L, 200L, 20L, 2L), 0L, responses))
        ).awaitUnchecked();

        long startTime = System.currentTimeMillis();
        long n = 0L + (10L*10L*10L*10L*10L) + 1;
        for (int i = 0; i < n; i++) {
            conjunctive.tell(actor ->
                    actor.receiveRequest(
                            new Request(new Plan(Arrays.asList(conjunctive)).toNextStep(), Arrays.asList(), Arrays.asList(), Arrays.asList())
                    )
            );
        }
        for (int i = 0; i < n - 1; i++) {
            Long answer = responses.take();
            assertTrue(answer != -1);
        }
        assertEquals(responses.take().longValue(), -1L);
        System.out.println("Time : " + (System.currentTimeMillis() - startTime));
        assertTrue(responses.isEmpty());
    }
}
