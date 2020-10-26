package grakn.common.poc;

import grakn.common.concurrent.NamedThreadFactory;
import grakn.common.concurrent.actor.Actor;
import grakn.common.concurrent.actor.ActorRoot;
import grakn.common.concurrent.actor.eventloop.EventLoopSingleThreaded;

import java.util.ArrayList;
import java.util.Iterator;

public class NonCooperativeTest {

    public static void main(String[] args) throws InterruptedException {
        EventLoopSingleThreaded eventLoop = new EventLoopSingleThreaded(NamedThreadFactory.create(NonCooperativeTest.class, "main"));
        ArrayList<Long> output = new ArrayList<>();
        Actor<ActorRoot> rootActor = Actor.root(eventLoop, ActorRoot::new);
        Actor<NoncooperativeStarter> starter = rootActor.ask(root -> root.<NoncooperativeStarter>createActor((self) -> new NoncooperativeStarter(self, output))).await();
        // kill manually
        starter.tell((actor) -> actor.start());
        Thread.sleep(10000);
    }
}

/**
 * A starter for the noncooperative actor that can repeatedly call run the non cooperative actor
 */
class NoncooperativeStarter extends Actor.State<NoncooperativeStarter> {
    private final Actor<NoncooperativeActor> noncooperativeActor;
    private long startTime;
    private ArrayList<Long> output;
    private int completed = 0;
    private int repeat = 1;

    protected NoncooperativeStarter(final Actor<NoncooperativeStarter> self, ArrayList<Long> output) {
        super(self);
        this.output = output;
        noncooperativeActor = this.<NoncooperativeActor>child((actor) -> new NoncooperativeActor(actor, output, self));
    }

    public void start() {
        this.startTime = System.currentTimeMillis();
        for (int i = 0; i < repeat; i++) {
            noncooperativeActor.tell((actor) -> actor.run());
        }
    }

    public void done() {
        completed++;
        if (completed == repeat) {
            long elapsed = System.currentTimeMillis() - startTime;
            System.out.println("Time taken for " + repeat + " noncooperative repeats (ms): " + (elapsed));
            System.out.println("Length of output array: " + (output.size()));
        }
    }
}


class NoncooperativeActor extends Actor.State<NoncooperativeActor> {

    private final ArrayList<Long> output;
    private Actor<NoncooperativeStarter> starter;
    MockTransaction tx;

    protected NoncooperativeActor(final Actor<NoncooperativeActor> self, ArrayList<Long> output, Actor<NoncooperativeStarter> parent) {
        super(self);
        this.output = output;
        this.starter = parent;
        tx = new MockTransaction(10000000, 100);
    }

    public void run() {
        Iterator<Long> answers = tx.query();
        answers.forEachRemaining(l -> send(l));
        starter.tell((actor) -> actor.done());
    }

    private void send(final Long l) {
        output.add(l);
    }
}



