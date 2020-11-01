package grakn.common.poc.reasoning;

import grakn.common.concurrent.actor.Actor;
import org.junit.Test;

import java.util.Arrays;

import static junit.framework.TestCase.assertTrue;

public class ReasoningTest {
    @Test
    public void singleAtomicActor() throws InterruptedException {
        ActorManager manager = new ActorManager();

        // create atomic actors first to control answer size
        manager.createAtomicActor(0L, 5L, Arrays.asList());
        Actor<ConjunctiveActor> conjunctive = manager.createRootConjunctiveActor(Arrays.asList(0L), 5L);

        long startTime = System.currentTimeMillis();
        int n = 5;
        for (int i = 0; i < n; i++) {
            conjunctive.tell(actor ->
                    actor.receiveRequest(
                            new Request(conjunctive.state.path, Arrays.asList(), Arrays.asList(), Arrays.asList())
                    )
            );
            manager.takeAnswer();
        }
        System.out.println("Time : " + (System.currentTimeMillis() - startTime));
        Thread.sleep(20);
    }


    @Test
    public void doubleAtomicActor() throws InterruptedException {
        ActorManager manager = new ActorManager();

        // create atomic actors first to control answer size
        manager.createAtomicActor(2L, 2L, Arrays.asList());
        manager.createAtomicActor(20L, 2L, Arrays.asList());
        Actor<ConjunctiveActor> conjunctive = manager.createRootConjunctiveActor(Arrays.asList(20L, 2L), 0L);

        long startTime = System.currentTimeMillis();
        int n = 4;
        for (int i = 0; i < n; i++) {
            conjunctive.tell(actor ->
                    actor.receiveRequest(
                            new Request(conjunctive.state.path, Arrays.asList(), Arrays.asList(), Arrays.asList())
                    )
            );
        }
//        Thread.sleep(1000); // enable for debugging to ensure equivalent debug vs normal execution
        manager.takeAnswer();
        manager.takeAnswer();
        manager.takeAnswer();
        manager.takeAnswer();
        System.out.println("Time : " + (System.currentTimeMillis() - startTime));
        assertTrue(!manager.hasAnswer());
    }

    @Test
    public void simpleRule() throws InterruptedException {
        // check TODO in ReasoningActor for why this is stalling
        ActorManager manager = new ActorManager();

        // create atomic actors first to control answer size
        Actor<AtomicActor> bottomAtomic = manager.createAtomicActor(-2L, 4L, Arrays.asList());
        Actor<AtomicActor> atomicWithRule = manager.createAtomicActor(2L, 4L, Arrays.asList(Arrays.asList(-2L)));
        Actor<ConjunctiveActor> rootConjunction = manager.createRootConjunctiveActor(Arrays.asList(2L), 0L);

        long startTime = System.currentTimeMillis();
        int n = 4;
        for (int i = 0; i < n; i++) {
            rootConjunction.tell(actor ->
                    actor.receiveRequest(
                            new Request(rootConjunction.state.path, Arrays.asList(), Arrays.asList(), Arrays.asList())
                    )
            );
        }
//        Thread.sleep(1000); // enable for debugging to ensure equivalent debug vs normal execution
        manager.takeAnswer();
        manager.takeAnswer();
        manager.takeAnswer();
        manager.takeAnswer();
        System.out.println("Time : " + (System.currentTimeMillis() - startTime));
        assertTrue(!manager.hasAnswer());
    }

    @Test
    public void atomicChainWithRule() throws InterruptedException {
        ActorManager manager = new ActorManager();

        // create atomic actors first to control answer size
        Actor<AtomicActor> bottomAtomic = manager.createAtomicActor(-2L, 4L, Arrays.asList());
        Actor<AtomicActor> atomicWithRule = manager.createAtomicActor(2L, 4L, Arrays.asList(Arrays.asList(-2L)));
        Actor<AtomicActor> atomic = manager.createAtomicActor(20L, 4L, Arrays.asList());
        Actor<ConjunctiveActor> rootConjunction = manager.createRootConjunctiveActor(Arrays.asList(20L, 2L), 0L);

        long startTime = System.currentTimeMillis();
        int n = 4;
        for (int i = 0; i < n; i++) {
            rootConjunction.tell(actor ->
                    actor.receiveRequest(
                            new Request(rootConjunction.state.path, Arrays.asList(), Arrays.asList(), Arrays.asList())
                    )
            );
        }
//        Thread.sleep(1000); // enable for debugging to ensure equivalent debug vs normal execution
        manager.takeAnswer();
        manager.takeAnswer();
        manager.takeAnswer();
        manager.takeAnswer();
        System.out.println("Time : " + (System.currentTimeMillis() - startTime));
        assertTrue(!manager.hasAnswer());
    }


    @Test
    public void shallowRerequest() throws InterruptedException {
        ActorManager manager = new ActorManager();

        // create atomic actors first to control answer size
        manager.createAtomicActor(2L, 2L, Arrays.asList());
        manager.createAtomicActor(20L, 2L, Arrays.asList());
        manager.createAtomicActor(200L, 2L, Arrays.asList());

        Actor<ConjunctiveActor> conjunctive = manager.createRootConjunctiveActor(Arrays.asList(200L, 20L, 2L), 0L);
        long startTime = System.currentTimeMillis();
        int n = 5;
        for (int i = 0; i < n; i++) {
            conjunctive.tell(actor ->
                    actor.receiveRequest(
                            // TODO UNDO HACK
                            new Request(conjunctive.state.path, Arrays.asList(), Arrays.asList(), Arrays.asList())
                    )
            );
        }
        for (int i = 0; i < n; i++) {
            manager.takeAnswer();
        }
        System.out.println("Time : " + (System.currentTimeMillis() - startTime));
        assertTrue(!manager.hasAnswer());
    }

    @Test
    public void deepRerequest() throws InterruptedException {
        ActorManager manager = new ActorManager();

        // create atomic actors first to control answer size
        manager.createAtomicActor(2L, 10L, Arrays.asList());
        manager.createAtomicActor(20L, 10L, Arrays.asList());
        manager.createAtomicActor(200L, 10L, Arrays.asList());
        manager.createAtomicActor(2000L, 10L, Arrays.asList());
        manager.createAtomicActor(20000L, 10L, Arrays.asList());
        Actor<ConjunctiveActor> conjunctive = manager.createRootConjunctiveActor(Arrays.asList(20000L, 2000L, 200L, 20L, 2L), 0L);

        long startTime = System.currentTimeMillis();
        int n = 10000;
        for (int i = 0; i < n; i++) {
            conjunctive.tell(actor ->
                    actor.receiveRequest(
                            new Request(conjunctive.state.path, Arrays.asList(), Arrays.asList(), Arrays.asList())
                    )
            );
        }
        for (int i = 0; i < n; i++) {
            manager.takeAnswer();
        }
        System.out.println("Time : " + (System.currentTimeMillis() - startTime));
        assertTrue(!manager.hasAnswer());
    }
}
