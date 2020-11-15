package grakn.common.poc.reasoning.test;

import grakn.common.concurrent.actor.Actor;
import grakn.common.concurrent.actor.ActorRoot;
import grakn.common.concurrent.actor.eventloop.EventLoopGroup;
import grakn.common.poc.reasoning.Registry;
import grakn.common.poc.reasoning.Atomic;
import grakn.common.poc.reasoning.Conjunction;
import grakn.common.poc.reasoning.Rule;
import grakn.common.poc.reasoning.execution.Request;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import static grakn.common.collection.Collections.list;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

public class ReasoningTest {
    @Test
    public void singleAtomicActor() throws InterruptedException {
        Registry registry = new Registry();

        LinkedBlockingQueue<List<Long>> responses = new LinkedBlockingQueue<>();
        EventLoopGroup eventLoop = new EventLoopGroup(1, "reasoning-elg");
        Actor<ActorRoot> rootActor = Actor.root(eventLoop, ActorRoot::new);

        // create atomic actors first to control answer size
        registry.registerAtomic(0L, pattern ->
                rootActor.ask(actor ->
                        actor.<Atomic>createActor(self -> new Atomic(self, pattern, 5L, list()))
                ).awaitUnchecked()
        );
        Actor<Conjunction> conjunctive = rootActor.ask(actor ->
                actor.<Conjunction>createActor(self -> new Conjunction(self, list(0L), 5L, responses))
        ).awaitUnchecked();


        long startTime = System.currentTimeMillis();
        long n = 5L + 5L + 1; //total number of traversal answers
        for (int i = 0; i < n; i++) {
            conjunctive.tell(actor ->
                    actor.executeReceiveRequest(
                            new Request(new Request.Path(conjunctive), list(), list(), list()),
                            registry
                    )
            );
            List<Long> answer = responses.take();
            System.out.println(answer);
            if (i < n - 1) {
                assertTrue(!answer.isEmpty());
            } else {
                assertTrue(answer.isEmpty());
            }
        }
        System.out.println("Time : " + (System.currentTimeMillis() - startTime));
        Thread.sleep(20);
    }

    @Test
    public void doubleAtomicActor() throws InterruptedException {
        Registry registry = new Registry();

        LinkedBlockingQueue<List<Long>> responses = new LinkedBlockingQueue<>();
        EventLoopGroup eventLoop = new EventLoopGroup(1, "reasoning-elg");
        Actor<ActorRoot> rootActor = Actor.root(eventLoop, ActorRoot::new);

        // create atomic actors first to control answer size
        registry.registerAtomic(2L, pattern ->
                rootActor.ask(actor ->
                        actor.<Atomic>createActor(self -> new Atomic(self, pattern, 2L, list()))
                ).awaitUnchecked()
        );
        registry.registerAtomic(20L, pattern ->
                rootActor.ask(actor ->
                        actor.<Atomic>createActor(self -> new Atomic(self, pattern, 2L, list()))
                ).awaitUnchecked()
        );
        Actor<Conjunction> conjunctive = rootActor.ask(actor ->
                actor.<Conjunction>createActor(self -> new Conjunction(self, list(20L, 2L), 0L, responses))
        ).awaitUnchecked();


        long startTime = System.currentTimeMillis();
        long n = 0L + (2L * 2L) + 1; //total number of traversal answers, plus one expected DONE (-1 answer)
        for (int i = 0; i < n; i++) {
            conjunctive.tell(actor ->
                    actor.executeReceiveRequest(
                            new Request(new Request.Path(conjunctive), list(), list(), list()),
                            registry
                    )
            );
        }

        for (int i = 0; i < n - 1; i++) {
            List<Long> answer = responses.take();
            assertTrue(!answer.isEmpty());
        }
        assertEquals(responses.take(), list());
//        Thread.sleep(1000); // enable for debugging to ensure equivalent debug vs normal execution
        System.out.println("Time : " + (System.currentTimeMillis() - startTime));
        assertTrue(responses.isEmpty());
    }

    @Test
    public void filteringAtomicActor() throws InterruptedException {
        Registry registry = new Registry();

        LinkedBlockingQueue<List<Long>> responses = new LinkedBlockingQueue<>();
        EventLoopGroup eventLoop = new EventLoopGroup(1, "reasoning-elg");
        Actor<ActorRoot> rootActor = Actor.root(eventLoop, ActorRoot::new);

        // create atomic actors first to control answer size
        registry.registerAtomic(2L, pattern ->
                rootActor.ask(actor ->
                        actor.<Atomic>createActor(self -> new Atomic(self, pattern, 2L, list()))
                ).awaitUnchecked()
        );
        registry.registerAtomic(20L, pattern ->
                rootActor.ask(actor ->
                        actor.<Atomic>createActor(self -> new Atomic(self, pattern, 0L, list()))
                ).awaitUnchecked()
        );
        Actor<Conjunction> conjunctive = rootActor.ask(actor ->
                actor.<Conjunction>createActor(self -> new Conjunction(self, list(20L, 2L), 0L, responses))
        ).awaitUnchecked();


        long startTime = System.currentTimeMillis();
        long n = 0L + (0L * 0L) + 1; // total number of traversal answers, plus one expected DONE (-1 answer)
        for (int i = 0; i < n; i++) {
            conjunctive.tell(actor ->
                    actor.executeReceiveRequest(
                            new Request(new Request.Path(conjunctive), list(), list(), list()),
                            registry
                    )
            );
        }

        for (int i = 0; i < n - 1; i++) {
            List<Long> answer = responses.take();
            assertTrue(!answer.isEmpty());
        }
        assertEquals(responses.take(), list());
//        Thread.sleep(1000); // enable for debugging to ensure equivalent debug vs normal execution
        System.out.println("Time : " + (System.currentTimeMillis() - startTime));
        assertTrue(responses.isEmpty());
    }

    @Test
    public void simpleRule() throws InterruptedException {
        Registry registry = new Registry();

        LinkedBlockingQueue<List<Long>> responses = new LinkedBlockingQueue<>();
        EventLoopGroup eventLoop = new EventLoopGroup(1, "reasoning-elg");
        Actor<ActorRoot> rootActor = Actor.root(eventLoop, ActorRoot::new);

        // create atomic actors first to control answer size
        registry.registerAtomic(-2L, pattern ->
                rootActor.ask(actor ->
                        actor.<Atomic>createActor(self -> new Atomic(self, pattern, 1L, list()))
                ).awaitUnchecked()
        );
        registry.registerRule(list(-2L), pattern ->
                rootActor.ask(actor ->
                        actor.<Rule>createActor(self -> new Rule(self, pattern, 1L))
                ).awaitUnchecked()
        );
        registry.registerAtomic(2L, pattern ->
                rootActor.ask(actor ->
                        actor.<Atomic>createActor(self -> new Atomic(self, pattern, 1L, list(list(-2L))))
                ).awaitUnchecked()
        );
        Actor<Conjunction> conjunctive = rootActor.ask(actor ->
                actor.<Conjunction>createActor(self -> new Conjunction(self, list(2L), 0L, responses))
        ).awaitUnchecked();

        long startTime = System.currentTimeMillis();
        long n = 0L + (1L) + (1L) + (1L) + 1; //total number of traversal answers, plus one expected DONE (-1 answer)
        for (int i = 0; i < n; i++) {
            conjunctive.tell(actor ->
                    actor.executeReceiveRequest(
                            new Request(new Request.Path(conjunctive), list(), list(), list()),
                            registry
                    )
            );
        }

        for (int i = 0; i < n - 1; i++) {
            List<Long> answer = responses.take();
            assertTrue(!answer.isEmpty());
        }
        assertEquals(responses.take(), list());
        System.out.println("Time : " + (System.currentTimeMillis() - startTime));
        assertTrue(responses.isEmpty());
    }

    @Test
    public void atomicChainWithRule() throws InterruptedException {
        Registry registry = new Registry();

        LinkedBlockingQueue<List<Long>> responses = new LinkedBlockingQueue<>();
        EventLoopGroup eventLoop = new EventLoopGroup(1, "reasoning-elg");
        Actor<ActorRoot> rootActor = Actor.root(eventLoop, ActorRoot::new);

        // create atomic actors first to control answer size
        Actor<Atomic> bottomAtomic = registry.registerAtomic(-2L, pattern ->
                rootActor.ask(actor ->
                        actor.<Atomic>createActor(self -> new Atomic(self, pattern, 1L, list()))
                ).awaitUnchecked()
        );
        registry.registerRule(list(-2L), pattern ->
                rootActor.ask(actor ->
                        actor.<Rule>createActor(self -> new Rule(self, pattern, 1L))
                ).awaitUnchecked()
        );
        Actor<Atomic> atomicWithRule = registry.registerAtomic(2L, pattern ->
                rootActor.ask(actor ->
                        actor.<Atomic>createActor(self -> new Atomic(self, pattern, 1L, list(list(-2L))))
                ).awaitUnchecked()
        );
        Actor<Atomic> atomicWithoutRule = registry.registerAtomic(20L, pattern ->
                rootActor.ask(actor ->
                        actor.<Atomic>createActor(self -> new Atomic(self, pattern, 1L, list()))
                ).awaitUnchecked()
        );
        Actor<Conjunction> conjunctive = rootActor.ask(actor ->
                actor.<Conjunction>createActor(self -> new Conjunction(self, list(20L, 2L), 0L, responses))
        ).awaitUnchecked();

        long startTime = System.currentTimeMillis();
        long n = 0L + (1 * 1) + (1 * 2) + 1; //total number of traversal answers, plus one expected DONE (-1 answer)
        for (int i = 0; i < n; i++) {
            conjunctive.tell(actor ->
                    actor.executeReceiveRequest(
                            new Request(new Request.Path(conjunctive), list(), list(), list()),
                            registry
                    )
            );
        }

        for (int i = 0; i < n - 1; i++) {
            List<Long> answer = responses.take();
            System.out.println("---- take(): " + answer);
            assertTrue(!answer.isEmpty());
        }
        assertEquals(responses.take(), list());
        System.out.println("Time : " + (System.currentTimeMillis() - startTime));
        assertTrue(responses.isEmpty());
    }

    @Test
    public void shallowRerequest() throws InterruptedException {
        Registry registry = new Registry();

        LinkedBlockingQueue<List<Long>> responses = new LinkedBlockingQueue<>();
        EventLoopGroup eventLoop = new EventLoopGroup(1, "reasoning-elg");
        Actor<ActorRoot> rootActor = Actor.root(eventLoop, ActorRoot::new);

        // create atomic actors first to control answer size
        registry.registerAtomic(2L, pattern ->
                rootActor.ask(actor ->
                        actor.<Atomic>createActor(self -> new Atomic(self, pattern, 2L, list()))
                ).awaitUnchecked()
        );
        registry.registerAtomic(20L, pattern ->
                rootActor.ask(actor ->
                        actor.<Atomic>createActor(self -> new Atomic(self, pattern, 2L, list()))
                ).awaitUnchecked()
        );
        registry.registerAtomic(200L, pattern ->
                rootActor.ask(actor ->
                        actor.<Atomic>createActor(self -> new Atomic(self, pattern, 2L, list()))
                ).awaitUnchecked()
        );
        Actor<Conjunction> conjunctive = rootActor.ask(actor ->
                actor.<Conjunction>createActor(self -> new Conjunction(self, list(200L, 20L, 2L), 0L, responses))
        ).awaitUnchecked();

        long startTime = System.currentTimeMillis();
        long n = 0L + (2L * 2L * 2L) + 1;
        for (int i = 0; i < n; i++) {
            conjunctive.tell(actor ->
                    actor.executeReceiveRequest(
                            new Request(new Request.Path(conjunctive), list(), list(), list()),
                            registry
                    )
            );
        }
        for (int i = 0; i < n - 1; i++) {
            List<Long> answer = responses.take();
            assertFalse(answer.isEmpty());
        }
        assertEquals(responses.take(), list());
        System.out.println("Time : " + (System.currentTimeMillis() - startTime));
        assertTrue(responses.isEmpty());
    }

    @Test
    public void deepRerequest() throws InterruptedException {
        Registry registry = new Registry();

        LinkedBlockingQueue<List<Long>> responses = new LinkedBlockingQueue<>();
        EventLoopGroup eventLoop = new EventLoopGroup(1, "reasoning-elg");
        Actor<ActorRoot> rootActor = Actor.root(eventLoop, ActorRoot::new);

        // create atomic actors first to control answer size
        registry.registerAtomic(2L, pattern ->
                rootActor.ask(actor ->
                        actor.<Atomic>createActor(self -> new Atomic(self, pattern, 10L, list()))
                ).awaitUnchecked()
        );
        registry.registerAtomic(20L, pattern ->
                rootActor.ask(actor ->
                        actor.<Atomic>createActor(self -> new Atomic(self, pattern, 10L, list()))
                ).awaitUnchecked()
        );
        registry.registerAtomic(200L, pattern ->
                rootActor.ask(actor ->
                        actor.<Atomic>createActor(self -> new Atomic(self, pattern, 10L, list()))
                ).awaitUnchecked()
        );
        registry.registerAtomic(2000L, pattern ->
                rootActor.ask(actor ->
                        actor.<Atomic>createActor(self -> new Atomic(self, pattern, 10L, list()))
                ).awaitUnchecked()
        );
        registry.registerAtomic(20000L, pattern ->
                rootActor.ask(actor ->
                        actor.<Atomic>createActor(self -> new Atomic(self, pattern, 10L, list()))
                ).awaitUnchecked()
        );
        Actor<Conjunction> conjunctive = rootActor.ask(actor ->
                actor.<Conjunction>createActor(self -> new Conjunction(self, list(20000L, 2000L, 200L, 20L, 2L), 0L, responses))
        ).awaitUnchecked();

        long startTime = System.currentTimeMillis();
        long n = 0L + (10L * 10L * 10L * 10L * 10L) + 1;
        System.out.println("Number of requests: " + n);
        for (int i = 0; i < n; i++) {
            conjunctive.tell(actor ->
                    actor.executeReceiveRequest(
                            new Request(new Request.Path(conjunctive), list(), list(), list()),
                            registry
                    )
            );
        }
        System.out.println("Starting to take");
        for (int i = 0; i < n - 1; i++) {
            List<Long> answer = responses.take();
            assertTrue(!answer.isEmpty());
        }
        assertEquals(responses.take(), list());
        System.out.println("Time : " + (System.currentTimeMillis() - startTime));
        assertTrue(responses.isEmpty());
    }

    @Test
    public void bulkActorCreation() throws InterruptedException {
        Registry registry = new Registry();
        LinkedBlockingQueue<List<Long>> responses = new LinkedBlockingQueue<>();
        EventLoopGroup eventLoop = new EventLoopGroup(1, "reasoning-elg");
        Actor<ActorRoot> rootActor = Actor.root(eventLoop, ActorRoot::new);

        List<List<Long>> rules = new ArrayList<>();
        for (long i = 2L; i < 1000_000L; i++) {
            rules.add(list(i));
        }
        long start = System.currentTimeMillis();
        registry.registerAtomic(1L, pattern ->
                rootActor.ask(actor -> actor.<Atomic>createActor(self -> new Atomic(self, pattern, 1L, rules))).awaitUnchecked()
        );
        Actor<Conjunction> conjunctive = rootActor.ask(actor -> actor.<Conjunction>createActor(self -> new Conjunction(self, list(1L), 0L, responses))).awaitUnchecked();
        conjunctive.tell(actor ->
                actor.executeReceiveRequest(
                        new Request(new Request.Path(conjunctive), list(), list(), list()),
                        registry
                )
        );
        responses.take();
        long elapsed = System.currentTimeMillis() - start;
        System.out.println("elapsed = " + elapsed);
    }

    @Test
    public void loopTermination() throws InterruptedException {
        Registry registry = new Registry();
        LinkedBlockingQueue<List<Long>> responses = new LinkedBlockingQueue<>();
        EventLoopGroup eventLoop = new EventLoopGroup(1, "reasoning-elg");
        Actor<ActorRoot> rootActor = Actor.root(eventLoop, ActorRoot::new);

        // conjunction1 -> atomic1 -> rule1 -> atomic1
        registry.registerRule(list(1L), pattern -> rootActor.ask(actor -> actor.<Rule>createActor(self -> new Rule(self, pattern, 1L))).awaitUnchecked());
        registry.registerAtomic(1L, pattern -> rootActor.ask(actor -> actor.<Atomic>createActor(self -> new Atomic(self, pattern, 1L, list(list(1L))))).awaitUnchecked());
        Actor<Conjunction> conjunctive = rootActor.ask(actor -> actor.<Conjunction>createActor(self -> new Conjunction(self, list(1L), 0L, responses))).awaitUnchecked();

        long n = 0L + 1L + 1L + 1L + 1;
        for (int i = 0; i < n; i++) {
            conjunctive.tell(actor ->
                    actor.executeReceiveRequest(
                            new Request(new Request.Path(conjunctive), list(), list(), list()),
                            registry
                    )
            );
        }
        for (int i = 0; i < n - 1; i++) {
            List<Long> answer = responses.take();
            assertTrue(!answer.isEmpty());
        }
        assertEquals(responses.take(), list());
        assertTrue(responses.isEmpty());
    }

}
