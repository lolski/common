package grakn.common.poc.reasoning;

import grakn.common.concurrent.actor.Actor;
import org.junit.Test;

import java.util.Arrays;

import static junit.framework.TestCase.assertTrue;

public class AtomicActorTest {
    @Test
    public void singleActor() throws InterruptedException {
        ActorManager manager = new ActorManager();
        Actor<AtomicActor> atomic = manager.createAtomicActor("A", 0L, 2L, true);

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
        Actor<AtomicActor> atomic = manager.createAtomicActor( "B", 2L, 2L, true);
        Actor<AtomicActor> subAtomic = manager.createAtomicActor("A", 20L, 2L, false);

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
        Actor<AtomicActor> atomic = manager.createAtomicActor( "3", 2L, 1L, true);
        Actor<AtomicActor> subAtomic1 = manager.createAtomicActor( "2", 20L, 1L, false);
        Actor<AtomicActor> subAtomic2 = manager.createAtomicActor( "1", 200L, 1L, false);

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
            manager.takeAnswer();
        }
        System.out.println("Time : " + (System.currentTimeMillis() - startTime));
        assertTrue(!manager.hasAnswer());
    }

    @Test
    public void deepRerequest() throws InterruptedException {
        ActorManager manager = new ActorManager();

        Actor<AtomicActor> atomic = manager.createAtomicActor(  "5", 2L, 10L, true);
        Actor<AtomicActor> subAtomic = manager.createAtomicActor( "4", 20L, 10L, false);
        Actor<AtomicActor> subAtomic1 = manager.createAtomicActor( "3", 200L, 10L, false);
        Actor<AtomicActor> subAtomic2 = manager.createAtomicActor( "2", 2000L, 10L, false);
        Actor<AtomicActor> subAtomic3 = manager.createAtomicActor( "1", 20000L, 10L, false);

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
