package grakn.common.poc.reasoning;

import grakn.common.collection.Collections;
import grakn.common.concurrent.actor.Actor;
import grakn.common.concurrent.actor.ActorRoot;
import grakn.common.concurrent.actor.eventloop.EventLoopGroup;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

import static grakn.common.collection.Collections.list;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

public class ReasoningTest {
    @Test
    public void singleAtomicActor() throws InterruptedException {
        ActorRegistry actorRegistry = new ActorRegistry();

        LinkedBlockingQueue<Long> responses = new LinkedBlockingQueue<>();
        EventLoopGroup eventLoop = new EventLoopGroup(1, "reasoning-elg");
        Actor<ActorRoot> rootActor = Actor.root(eventLoop, ActorRoot::new);

        // create atomic actors first to control answer size
        actorRegistry.registerAtomic(0L, pattern ->
                rootActor.ask(actor ->
                        actor.<AtomicActor>createActor(self -> new AtomicActor(self, pattern, 5L, Arrays.asList()))
                ).awaitUnchecked()
        );
        Actor<ConjunctiveActor> conjunctive = rootActor.ask(actor ->
                actor.<ConjunctiveActor>createActor(self -> new ConjunctiveActor(self, Arrays.asList(0L), 5L, responses))
        ).awaitUnchecked();


        long startTime = System.currentTimeMillis();
        long n = 5L + 5L + 1; //total number of traversal answers
        for (int i = 0; i < n; i++) {
            conjunctive.tell(actor ->
                    actor.executeReceiveRequest(
                            new Request(new Plan(Arrays.asList(conjunctive)).toNextStep(), Arrays.asList(), Arrays.asList(), Arrays.asList()),
                            actorRegistry
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
        EventLoopGroup eventLoop = new EventLoopGroup(1, "reasoning-elg");
        Actor<ActorRoot> rootActor = Actor.root(eventLoop, ActorRoot::new);

        // create atomic actors first to control answer size
        actorRegistry.registerAtomic(2L, pattern ->
                rootActor.ask(actor ->
                        actor.<AtomicActor>createActor(self -> new AtomicActor(self, pattern, 2L, Arrays.asList()))
                ).awaitUnchecked()
        );
        actorRegistry.registerAtomic(20L, pattern ->
                rootActor.ask(actor ->
                        actor.<AtomicActor>createActor(self -> new AtomicActor(self, pattern, 2L, Arrays.asList()))
                ).awaitUnchecked()
        );
        Actor<ConjunctiveActor> conjunctive = rootActor.ask(actor ->
                actor.<ConjunctiveActor>createActor(self -> new ConjunctiveActor(self, Arrays.asList(20L, 2L), 0L, responses))
        ).awaitUnchecked();


        long startTime = System.currentTimeMillis();
        long n = 0L + (2L * 2L) + 1; //total number of traversal answers, plus one expected DONE (-1 answer)
        for (int i = 0; i < n; i++) {
            conjunctive.tell(actor ->
                    actor.executeReceiveRequest(
                            new Request(new Plan(Arrays.asList(conjunctive)).toNextStep(), Arrays.asList(), Arrays.asList(), Arrays.asList()),
                            actorRegistry
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
        EventLoopGroup eventLoop = new EventLoopGroup(1, "reasoning-elg");
        Actor<ActorRoot> rootActor = Actor.root(eventLoop, ActorRoot::new);

        // create atomic actors first to control answer size
        actorRegistry.registerAtomic(2L, pattern ->
                rootActor.ask(actor ->
                        actor.<AtomicActor>createActor(self -> new AtomicActor(self, pattern, 2L, Arrays.asList()))
                ).awaitUnchecked()
        );
        actorRegistry.registerAtomic(20L, pattern ->
                rootActor.ask(actor ->
                        actor.<AtomicActor>createActor(self -> new AtomicActor(self, pattern, 0L, Arrays.asList()))
                ).awaitUnchecked()
        );
        Actor<ConjunctiveActor> conjunctive = rootActor.ask(actor ->
                actor.<ConjunctiveActor>createActor(self -> new ConjunctiveActor(self, Arrays.asList(20L, 2L), 0L, responses))
        ).awaitUnchecked();


        long startTime = System.currentTimeMillis();
        long n = 0L + (0L * 0L) + 1; // total number of traversal answers, plus one expected DONE (-1 answer)
        for (int i = 0; i < n; i++) {
            conjunctive.tell(actor ->
                    actor.executeReceiveRequest(
                            new Request(new Plan(Arrays.asList(conjunctive)).toNextStep(), Arrays.asList(), Arrays.asList(), Arrays.asList()),
                            actorRegistry
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
        EventLoopGroup eventLoop = new EventLoopGroup(1, "reasoning-elg");
        Actor<ActorRoot> rootActor = Actor.root(eventLoop, ActorRoot::new);

        // create atomic actors first to control answer size
        actorRegistry.registerAtomic(-2L, pattern ->
                rootActor.ask(actor ->
                        actor.<AtomicActor>createActor(self -> new AtomicActor(self, pattern, 1L, Arrays.asList()))
                ).awaitUnchecked()
        );
        actorRegistry.registerAtomic(2L, pattern ->
                rootActor.ask(actor ->
                        actor.<AtomicActor>createActor(self -> new AtomicActor(self, pattern, 1L, Arrays.asList(Arrays.asList(-2L))))
                ).awaitUnchecked()
        );
        Actor<ConjunctiveActor> conjunctive = rootActor.ask(actor ->
                actor.<ConjunctiveActor>createActor(self -> new ConjunctiveActor(self, Arrays.asList(2L), 0L, responses))
        ).awaitUnchecked();

        long startTime = System.currentTimeMillis();
        long n = 0L + (1L) + (1L) + (1L) + 1; //total number of traversal answers, plus one expected DONE (-1 answer)
        for (int i = 0; i < n; i++) {
            conjunctive.tell(actor ->
                    actor.executeReceiveRequest(
                            new Request(new Plan(Arrays.asList(conjunctive)).toNextStep(), Arrays.asList(), Arrays.asList(), Arrays.asList()),
                            actorRegistry
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
        EventLoopGroup eventLoop = new EventLoopGroup(1, "reasoning-elg");
        Actor<ActorRoot> rootActor = Actor.root(eventLoop, ActorRoot::new);

        // create atomic actors first to control answer size
        Actor<AtomicActor> bottomAtomic = actorRegistry.registerAtomic(-2L, pattern ->
                rootActor.ask(actor ->
                        actor.<AtomicActor>createActor(self -> new AtomicActor(self, pattern, 1L, Arrays.asList()))
                ).awaitUnchecked()
        );
        Actor<AtomicActor> atomicWithRule = actorRegistry.registerAtomic(2L, pattern ->
                rootActor.ask(actor ->
                        actor.<AtomicActor>createActor(self -> new AtomicActor(self, pattern, 1L, Arrays.asList(Arrays.asList(-2L))))
                ).awaitUnchecked()
        );
        Actor<AtomicActor> atomicWithoutRule = actorRegistry.registerAtomic(20L, pattern ->
                rootActor.ask(actor ->
                        actor.<AtomicActor>createActor(self -> new AtomicActor(self, pattern, 1L, Arrays.asList()))
                ).awaitUnchecked()
        );
        Actor<ConjunctiveActor> conjunctive = rootActor.ask(actor ->
                actor.<ConjunctiveActor>createActor(self -> new ConjunctiveActor(self, Arrays.asList(20L, 2L), 0L, responses))
        ).awaitUnchecked();

        long startTime = System.currentTimeMillis();
        long n = 0L + (1 * 1) + (1 * 2) + 1; //total number of traversal answers, plus one expected DONE (-1 answer)
        for (int i = 0; i < n; i++) {
            conjunctive.tell(actor ->
                    actor.executeReceiveRequest(
                            new Request(new Plan(Arrays.asList(conjunctive)).toNextStep(), Arrays.asList(), Arrays.asList(), Arrays.asList()),
                            actorRegistry
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
        EventLoopGroup eventLoop = new EventLoopGroup(1, "reasoning-elg");
        Actor<ActorRoot> rootActor = Actor.root(eventLoop, ActorRoot::new);

        // create atomic actors first to control answer size
        actorRegistry.registerAtomic(2L, pattern ->
                rootActor.ask(actor ->
                        actor.<AtomicActor>createActor(self -> new AtomicActor(self, pattern, 4L, Arrays.asList()))
                ).awaitUnchecked()
        );
        actorRegistry.registerAtomic(20L, pattern ->
                rootActor.ask(actor ->
                        actor.<AtomicActor>createActor(self -> new AtomicActor(self, pattern, 4L, Arrays.asList()))
                ).awaitUnchecked()
        );
        actorRegistry.registerAtomic(200L, pattern ->
                rootActor.ask(actor ->
                        actor.<AtomicActor>createActor(self -> new AtomicActor(self, pattern, 4L, Arrays.asList()))
                ).awaitUnchecked()
        );
        Actor<ConjunctiveActor> conjunctive = rootActor.ask(actor ->
                actor.<ConjunctiveActor>createActor(self -> new ConjunctiveActor(self, Arrays.asList(200L, 20L, 2L), 0L, responses))
        ).awaitUnchecked();

        long startTime = System.currentTimeMillis();
        long n = 0L + (4L * 4L * 4L) + 1;
        for (int i = 0; i < n; i++) {
            conjunctive.tell(actor ->
                    actor.executeReceiveRequest(
                            new Request(new Plan(Arrays.asList(conjunctive)).toNextStep(), Arrays.asList(), Arrays.asList(), Arrays.asList()),
                            actorRegistry
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
        EventLoopGroup eventLoop = new EventLoopGroup(1, "reasoning-elg");
        Actor<ActorRoot> rootActor = Actor.root(eventLoop, ActorRoot::new);

        // create atomic actors first to control answer size
        actorRegistry.registerAtomic(2L, pattern ->
                rootActor.ask(actor ->
                        actor.<AtomicActor>createActor(self -> new AtomicActor(self, pattern, 10L, Arrays.asList()))
                ).awaitUnchecked()
        );
        actorRegistry.registerAtomic(20L, pattern ->
                rootActor.ask(actor ->
                        actor.<AtomicActor>createActor(self -> new AtomicActor(self, pattern, 10L, Arrays.asList()))
                ).awaitUnchecked()
        );
        actorRegistry.registerAtomic(200L, pattern ->
                rootActor.ask(actor ->
                        actor.<AtomicActor>createActor(self -> new AtomicActor(self, pattern, 10L, Arrays.asList()))
                ).awaitUnchecked()
        );
        actorRegistry.registerAtomic(2000L, pattern ->
                rootActor.ask(actor ->
                        actor.<AtomicActor>createActor(self -> new AtomicActor(self, pattern, 10L, Arrays.asList()))
                ).awaitUnchecked()
        );
        actorRegistry.registerAtomic(20000L, pattern ->
                rootActor.ask(actor ->
                        actor.<AtomicActor>createActor(self -> new AtomicActor(self, pattern, 10L, Arrays.asList()))
                ).awaitUnchecked()
        );
        Actor<ConjunctiveActor> conjunctive = rootActor.ask(actor ->
                actor.<ConjunctiveActor>createActor(self -> new ConjunctiveActor(self, Arrays.asList(20000L, 2000L, 200L, 20L, 2L), 0L, responses))
        ).awaitUnchecked();

        long startTime = System.currentTimeMillis();
        long n = 0L + (10L * 10L * 10L * 10L * 10L) + 1;
        for (int i = 0; i < n; i++) {
            conjunctive.tell(actor ->
                    actor.executeReceiveRequest(
                            new Request(new Plan(Arrays.asList(conjunctive)).toNextStep(), Arrays.asList(), Arrays.asList(), Arrays.asList()),
                            actorRegistry
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
    public void bulkActorCreation() throws InterruptedException {
        ActorRegistry actorRegistry = new ActorRegistry();
        LinkedBlockingQueue<Long> responses = new LinkedBlockingQueue<>();
        EventLoopGroup eventLoop = new EventLoopGroup(1, "reasoning-elg");
        Actor<ActorRoot> rootActor = Actor.root(eventLoop, ActorRoot::new);

        List<List<Long>> rules = new ArrayList<>();
        for (long i = 2L; i < 1000_000L; i++) {
            rules.add(Arrays.asList(i));
        }
        long start = System.currentTimeMillis();
        actorRegistry.registerAtomic(1L, pattern ->
                rootActor.ask(actor -> actor.<AtomicActor>createActor(self -> new AtomicActor(self, pattern, 1L, rules))).awaitUnchecked()
        );
        Actor<ConjunctiveActor> conjunctive = rootActor.ask(actor -> actor.<ConjunctiveActor>createActor(self -> new ConjunctiveActor(self, Arrays.asList(1L), 0L, responses))).awaitUnchecked();
        conjunctive.tell(actor ->
                actor.executeReceiveRequest(
                        new Request(new Plan(Arrays.asList(conjunctive)).toNextStep(), Arrays.asList(), Arrays.asList(), Arrays.asList()),
                        actorRegistry
                )
        );
        responses.take();
        long elapsed = System.currentTimeMillis() - start;
        System.out.println("elapsed = " + elapsed);
    }

    @Test
    public void loopTermination() throws InterruptedException {
        ActorRegistry actorRegistry = new ActorRegistry();
        LinkedBlockingQueue<Long> responses = new LinkedBlockingQueue<>();
        EventLoopGroup eventLoop = new EventLoopGroup(1, "reasoning-elg");
        Actor<ActorRoot> rootActor = Actor.root(eventLoop, ActorRoot::new);

        // conjunction1 -> atomic1 -> rule1 -> conjunction2 -> atomic1
        actorRegistry.registerRule(list(1L), pattern -> rootActor.ask(actor -> actor.<RuleActor>createActor(self -> new RuleActor(self, pattern, 1L))).awaitUnchecked());
        actorRegistry.registerAtomic(1L, pattern -> rootActor.ask(actor -> actor.<AtomicActor>createActor(self -> new AtomicActor(self, pattern, 1L, list(list(1L))))).awaitUnchecked());
        Actor<ConjunctiveActor> conjunctive = rootActor.ask(actor -> actor.<ConjunctiveActor>createActor(self -> new ConjunctiveActor(self, Arrays.asList(1L), 0L, responses))).awaitUnchecked();

        long n = 0L + 1L + 1L + 1L + 1;
        for (int i = 0; i < n; i++) {
            conjunctive.tell(actor ->
                    actor.executeReceiveRequest(
                            new Request(new Plan(Arrays.asList(conjunctive)).toNextStep(), Arrays.asList(), Arrays.asList(), Arrays.asList()),
                            actorRegistry
                    )
            );
        }
        for (int i = 0; i < n - 1; i++) {
            Long answer = responses.take();
            assertTrue(answer != -1);
        }
        assertEquals(responses.take().longValue(), -1L);
        assertTrue(responses.isEmpty());
    }

}
