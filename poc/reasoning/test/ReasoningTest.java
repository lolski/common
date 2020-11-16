package grakn.common.poc.reasoning.test;

import grakn.common.concurrent.actor.Actor;
import grakn.common.concurrent.actor.eventloop.EventLoopGroup;
import grakn.common.poc.reasoning.Atomic;
import grakn.common.poc.reasoning.Conjunction;
import grakn.common.poc.reasoning.Registry;
import grakn.common.poc.reasoning.Rule;
import grakn.common.poc.reasoning.execution.Request;
import org.junit.Test;

import java.util.ArrayList;
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
        EventLoopGroup elg = new EventLoopGroup(1, "reasoning-elg");

        long atomicPattern = 0L;
        long atomicTraversalSize = 5L;
        registerAtomic(atomicPattern, list(), atomicTraversalSize, registry, elg);

        List<Long> conjunctionPattern = list(atomicPattern);
        long conjunctionTraversalSize = 5L;
        Actor<Conjunction> conjunction = registerConjunction(conjunctionPattern, conjunctionTraversalSize, -10L, responses, elg);

        assertResponsesSync(registry, responses, atomicTraversalSize + conjunctionTraversalSize, conjunction);
    }

    @Test
    public void doubleAtomicActors() throws InterruptedException {
        Registry registry = new Registry();
        LinkedBlockingQueue<List<Long>> responses = new LinkedBlockingQueue<>();
        EventLoopGroup elg = new EventLoopGroup(1, "reasoning-elg");

        long atomic1Pattern = 2L;
        long atomic1TraversalSize = 2L;
        registerAtomic(atomic1Pattern, list(), atomic1TraversalSize, registry, elg);

        long atomic2Pattern = 20L;
        long atomic2TraversalSize = 2L;
        registerAtomic(atomic2Pattern, list(), atomic2TraversalSize, registry, elg);

        List<Long> conjunctionPattern = list(atomic2Pattern, atomic1Pattern);
        long conjunctionTraversalSize = 0L;
        Actor<Conjunction> conjunction = registerConjunction(conjunctionPattern, conjunctionTraversalSize,0L,  responses, elg);

        assertResponses(registry, responses, conjunction, conjunctionTraversalSize + (atomic2TraversalSize * atomic1TraversalSize));
    }

    @Test
    public void filteringAtomicActor() throws InterruptedException {
        Registry registry = new Registry();
        LinkedBlockingQueue<List<Long>> responses = new LinkedBlockingQueue<>();
        EventLoopGroup elg = new EventLoopGroup(1, "reasoning-elg");

        long atomic1Pattern = 2L;
        long atomic1TraversalSize = 2L;
        registerAtomic(atomic1Pattern, list(), atomic1TraversalSize, registry, elg);

        long atomic2Pattern = 20L;
        long atomic2TraversalSize = 0L;
        registerAtomic(atomic2Pattern, list(), atomic2TraversalSize, registry, elg);

        List<Long> conjunctionPattern = list(atomic2Pattern, atomic1Pattern);
        long conjunctionTraversalSize = 0L;
        Actor<Conjunction> conjunction = registerConjunction(conjunctionPattern, conjunctionTraversalSize,0L,  responses, elg);

        assertResponses(registry, responses, conjunction, conjunctionTraversalSize + (atomic1TraversalSize * atomic2TraversalSize));
    }

    @Test
    public void simpleRule() throws InterruptedException {
        Registry registry = new Registry();
        LinkedBlockingQueue<List<Long>> responses = new LinkedBlockingQueue<>();
        EventLoopGroup elg = new EventLoopGroup(1, "reasoning-elg");

        long atomic1Pattern = -2L;
        long atomic1TraversalSize = 1L;
        registerAtomic(atomic1Pattern, list(), atomic1TraversalSize, registry, elg);

        List<Long> rulePattern = list(-2L);
        long ruleTraversalSize = 0L;
        long ruleTraversalOffset = 0L;
        registerRule(rulePattern, ruleTraversalSize, ruleTraversalOffset, registry, elg);

        long atomic2Pattern = 2L;
        long atomic2TraversalSize = 1L;
        registerAtomic(atomic2Pattern, list(rulePattern), atomic2TraversalSize, registry, elg);

        List<Long> conjunctionPattern = list(atomic2Pattern);
        long conjunctionTraversalSize = 0L;
        long conjunctionTraversalOffset = 0L;
        Actor<Conjunction> conjunction = registerConjunction(conjunctionPattern, conjunctionTraversalSize, conjunctionTraversalOffset, responses, elg);

        long answerCount = conjunctionTraversalSize + atomic2TraversalSize + ruleTraversalSize + atomic1TraversalSize;
        assertResponses(registry, responses, conjunction, answerCount);
    }

    @Test
    public void atomicChainWithRule() throws InterruptedException {
        Registry registry = new Registry();
        LinkedBlockingQueue<List<Long>> responses = new LinkedBlockingQueue<>();
        EventLoopGroup elg = new EventLoopGroup(1, "reasoning-elg");

        // create atomic actors first to control answer size
//        registry.registerAtomic(-2L, pattern ->
//                        Actor.create(elg, self -> new Atomic(self, pattern, list(), 1L)));
        long atomic1Pattern = -2L;
        long atomic1TraversalSize = 1L;
        registerAtomic(atomic1Pattern, list(), atomic1TraversalSize, registry, elg);

//        registry.registerRule(list(-2L), pattern ->
//                        Actor.create(elg, self -> new Rule(self, pattern, 1L, 0L)));
        List<Long> rulePattern = list(atomic1Pattern);
        long ruleTraversalSize = 1L;
        long ruleTraversalOffset = -10L;
        registerRule(rulePattern, ruleTraversalSize, ruleTraversalOffset, registry, elg);

//        registry.registerAtomic(2L, pattern ->
//                        Actor.create(elg, self -> new Atomic(self, pattern, list(list(-2L)), 1L)));
        long atomic2Pattern = 2L;
        long atomic2TraversalSize = 1L;
        registerAtomic(atomic2Pattern, list(rulePattern), atomic2TraversalSize, registry, elg);

//        registry.registerAtomic(20L, pattern ->
//                        Actor.create(elg, self -> new Atomic(self, pattern, list(), 1L)));
        long atomic3Pattern = 20L;
        long atomic3TraversalSize = 1L;
        registerAtomic(atomic3Pattern, list(), atomic3TraversalSize, registry, elg);

//        Actor<Conjunction> conjunction =
//                Actor.create(elg, self -> new Conjunction(self, list(20L, 2L), 0L, 0L, responses));
        List<Long> conjunctionPattern = list(atomic3Pattern, atomic2Pattern);
        long conjunctionTraversalSize = 0L;
        long conjunctionTraversalOffset = 0L;
        Actor<Conjunction> conjunction = registerConjunction(conjunctionPattern, conjunctionTraversalSize, conjunctionTraversalOffset, responses, elg);

//        long answerCount = 0L + (1 * 1) + (1 * 2); //total number of traversal answers, plus one expected DONE (-1 answer)
        long answerCount = conjunctionTraversalSize + atomic3TraversalSize + (atomic3TraversalSize * (atomic2TraversalSize + ruleTraversalSize + atomic1TraversalSize));
        assertResponses(registry, responses, conjunction, answerCount);
    }

    @Test
    public void shallowRerequest() throws InterruptedException {
        Registry registry = new Registry();
        LinkedBlockingQueue<List<Long>> responses = new LinkedBlockingQueue<>();
        EventLoopGroup elg = new EventLoopGroup(1, "reasoning-elg");

        // create atomic actors first to control answer size
        registry.registerAtomic(2L, pattern ->
                        Actor.create(elg, self -> new Atomic(self, pattern, list(), 2L)));

        registry.registerAtomic(20L, pattern ->
                        Actor.create(elg, self -> new Atomic(self, pattern, list(), 2L)));

        registry.registerAtomic(200L, pattern ->
                        Actor.create(elg, self -> new Atomic(self, pattern, list(), 2L)));

        Actor<Conjunction> conjunction =
                Actor.create(elg, self -> new Conjunction(self, list(200L, 20L, 2L), 0L, 0L, responses));
        long startTime = System.currentTimeMillis();
        long n = 0L + (2L * 2L * 2L) + 1;
        for (int i = 0; i < n; i++) {
            conjunction.tell(actor ->
                    actor.executeReceiveRequest(
                            new Request(new Request.Path(conjunction), list(), list(), list()),
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
        EventLoopGroup elg = new EventLoopGroup(1, "reasoning-elg");

        // create atomic actors first to control answer size
        registry.registerAtomic(2L, pattern ->
                        Actor.create(elg, self -> new Atomic(self, pattern, list(), 10L)));

        registry.registerAtomic(20L, pattern ->
                        Actor.create(elg, self -> new Atomic(self, pattern, list(), 10L)));

        registry.registerAtomic(200L, pattern ->
                        Actor.create(elg, self -> new Atomic(self, pattern, list(), 10L)));

        registry.registerAtomic(2000L, pattern ->
                        Actor.create(elg, self -> new Atomic(self, pattern, list(), 10L)));

        registry.registerAtomic(20000L, pattern ->
                        Actor.create(elg, self -> new Atomic(self, pattern, list(), 10L)));

        Actor<Conjunction> conjunction =
                Actor.create(elg, self -> new Conjunction(self, list(20000L, 2000L, 200L, 20L, 2L), 0L, 0L, responses));
        long startTime = System.currentTimeMillis();
        long n = 0L + (10L * 10L * 10L * 10L * 10L) + 1;
        System.out.println("Number of requests: " + n);
        for (int i = 0; i < n; i++) {
            conjunction.tell(actor ->
                    actor.executeReceiveRequest(
                            new Request(new Request.Path(conjunction), list(), list(), list()),
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
        EventLoopGroup elg = new EventLoopGroup(1, "reasoning-elg");

        List<List<Long>> rules = new ArrayList<>();
        for (long i = 2L; i < 1000_000L; i++) {
            rules.add(list(i));
        }
        long start = System.currentTimeMillis();
        registry.registerAtomic(1L, pattern ->
                Actor.create(elg, self -> new Atomic(self, pattern, rules, 1L))
        );
        Actor<Conjunction> conjunction = Actor.create(elg, self -> new Conjunction(self, list(1L), 0L, 0L, responses));
        conjunction.tell(actor ->
                actor.executeReceiveRequest(
                        new Request(new Request.Path(conjunction), list(), list(), list()),
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
        EventLoopGroup elg = new EventLoopGroup(1, "reasoning-elg");

        // conjunction1 -> atomic1 -> rule1 -> atomic1
        registry.registerRule(list(1L), pattern -> Actor.create(elg, self -> new Rule(self, pattern, 1L, 0L)));
        registry.registerAtomic(1L, pattern -> Actor.create(elg, self -> new Atomic(self, pattern, list(list(1L)), 1L)));
        Actor<Conjunction> conjunction = Actor.create(elg, self -> new Conjunction(self, list(1L), 0L, 0L, responses));

        long n = 0L + 1L + 1L + 1L + 1;
        for (int i = 0; i < n; i++) {
            conjunction.tell(actor ->
                    actor.executeReceiveRequest(
                            new Request(new Request.Path(conjunction), list(), list(), list()),
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

    @Test
    public void deduplication() throws InterruptedException {
        Registry registry = new Registry();
        LinkedBlockingQueue<List<Long>> responses = new LinkedBlockingQueue<>();
        EventLoopGroup elg = new EventLoopGroup(1, "reasoning-elg");

        registry.registerAtomic(1L, pattern ->

                        Actor.create(elg, self -> new Atomic(self, pattern, list(), 100L))
        );
        Actor<Conjunction> conjunction =
                Actor.create(elg, self -> new Conjunction(self, list(1L), 100L, 1L, responses));

        long n = 100L + 1L;
        for (int i = 0; i < n; i++) {
            conjunction.tell(actor ->
                    actor.executeReceiveRequest(
                            new Request(new Request.Path(conjunction), list(), list(), list()),
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

    private void registerAtomic(long pattern, List<List<Long>> rules, long traversalSize, Registry registry, EventLoopGroup elg) {
        registry.registerAtomic(pattern, p -> Actor.create(elg, self -> new Atomic(self, p, rules, traversalSize)));
    }

    private Actor<Conjunction> registerConjunction(List<Long> pattern, long traversalSize, long traversalOffset, LinkedBlockingQueue<List<Long>> responses, EventLoopGroup elg) {
        return Actor.create(elg, self -> new Conjunction(self, pattern, traversalSize, traversalOffset, responses));
    }

    private void registerRule(List<Long> pattern, long traversalSize, long traversalOffset, Registry registry, EventLoopGroup elg) {
        registry.registerRule(pattern, p -> Actor.create(elg, self -> new Rule(self, p, traversalSize, traversalOffset)));
    }

    private void assertResponses(Registry registry, LinkedBlockingQueue<List<Long>> responses, Actor<Conjunction> conjunction, long answerCount) throws InterruptedException {
        long startTime = System.currentTimeMillis();
        long n = answerCount + 1; //total number of traversal answers, plus one expected Exhausted (-1 answer)
        for (int i = 0; i < n; i++) {
            conjunction.tell(actor ->
                    actor.executeReceiveRequest(
                            new Request(new Request.Path(conjunction), list(), list(), list()),
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

    private void assertResponsesSync(Registry registry, LinkedBlockingQueue<List<Long>> responses, long answerCount, Actor<Conjunction> conjunction) throws InterruptedException {
        long startTime = System.currentTimeMillis();
        long n = answerCount + 1; //total number answers, plus one expected DONE (-1 answer)
        for (int i = 0; i < n; i++) {
            conjunction.tell(actor ->
                    actor.executeReceiveRequest(new Request(new Request.Path(conjunction), list(), list(), list()), registry));
            List<Long> answer = responses.take();
            System.out.println(answer);
            if (i < n - 1) {
                assertTrue(!answer.isEmpty());
            } else {
                assertTrue(answer.isEmpty());
            }
        }
        System.out.println("Time : " + (System.currentTimeMillis() - startTime));
    }
}
