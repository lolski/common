package grakn.common.poc.reasoning;

import grakn.common.concurrent.actor.Actor;
import org.junit.Test;

import java.util.Arrays;

import static junit.framework.TestCase.assertTrue;

public class AtomicActorTest {
    @Test
    public void singleActor() throws InterruptedException {
        ActorManager manager = new ActorManager();
        Actor<AtomicActor> atomic = manager.createAtomicActor(0L, 2L);

        long startTime = System.currentTimeMillis();
        int n = 5;
        for (int i = 0; i < n; i++) {
            atomic.tell(actor ->
                    actor.receiveRequest(
                            new Request(new Path(Arrays.asList(atomic)), Arrays.asList(), Arrays.asList(), Arrays.asList())
                    )
            );
            manager.takeAnswer();
        }
        System.out.println("Time : " + (System.currentTimeMillis() - startTime));
        Thread.sleep(20);
    }


    @Test
    public void basic() throws InterruptedException {
        ActorManager manager = new ActorManager();
        Actor<AtomicActor> atomic = manager.createAtomicActor(2L, 2L);
        Actor<AtomicActor> subAtomic = manager.createAtomicActor(20L, 2L);

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
        Actor<ConjunctiveActor> conjunctive = manager.createConjunctiveActor(Arrays.asList(200L, 20L, 2L));
        Actor<AtomicActor> atomic = manager.createAtomicActor(2L, 1L);
        Actor<AtomicActor> subAtomic1 = manager.createAtomicActor(20L, 1L);
        Actor<AtomicActor> subAtomic2 = manager.createAtomicActor(200L, 1L);

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

        Actor<AtomicActor> atomic = manager.createAtomicActor(2L, 10L);
        Actor<AtomicActor> subAtomic = manager.createAtomicActor(20L, 10L);
        Actor<AtomicActor> subAtomic1 = manager.createAtomicActor(200L, 10L);
        Actor<AtomicActor> subAtomic2 = manager.createAtomicActor(2000L, 10L);
        Actor<AtomicActor> subAtomic3 = manager.createAtomicActor(20000L, 10L);

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
            manager.takeAnswer();
        }
        System.out.println("Time : " + (System.currentTimeMillis() - startTime));
        assertTrue(!manager.hasAnswer());
    }
}
