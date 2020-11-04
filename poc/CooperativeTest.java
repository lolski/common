package grakn.common.poc;

import grakn.common.concurrent.actor.Actor;
import grakn.common.concurrent.actor.ActorRoot;
import grakn.common.concurrent.actor.eventloop.EventLoopGroup;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * Test performance and UX of incrementally computing a a job
 * The minimal packet of a job before it can be split up is one output of the source "iterator",
 * whatever that is. For instance, in Grakn it can be a query traversal.
 */
public class CooperativeTest {

    public static void main(String[] args) throws InterruptedException {
        EventLoopGroup eventLoop = new EventLoopGroup(1, "reasoning-elg");
        ArrayList<Long> output = new ArrayList<>();
        Actor<ActorRoot> rootActor = Actor.root(eventLoop, ActorRoot::new);
        Actor<CooperativeStarter> starter = rootActor.ask(root -> root.<CooperativeStarter>createActor((self) -> new CooperativeStarter(self, output))).await();
        starter.tell((actor) -> actor.start());
        // kill manually for now
        Thread.sleep(1000);
    }
}

class CooperativeStarter extends Actor.State<CooperativeStarter> {
    private final Actor<CooperativeActor> cooperative;
    private long startTime;
    private ArrayList<Long> output;
    private int completed = 0;
    private int repeat = 1;

    protected CooperativeStarter(final Actor<CooperativeStarter> self, ArrayList<Long> output) {
        super(self);
        this.output = output;
        cooperative = this.<CooperativeActor>child((actor) -> new CooperativeActor(actor, output, self));
    }

    public void start() {
        this.startTime = System.currentTimeMillis();
        for (int i = 0; i < repeat; i++) {
            cooperative.tell((actor) -> actor.run());
        }
    }

    public void done() {
        completed++;
        if (completed == repeat) {
            long elapsed = System.currentTimeMillis() - startTime;
            System.out.println("Time taken for " + repeat + " cooperative repeats (ms): " + (elapsed));
            System.out.println("Length of output array: " + (output.size()));
        }
    }
}

class CooperativeActor extends Actor.State<CooperativeActor> {

    private final MockTransaction tx;
    private final ArrayList<Long> output;
    private final Actor<CooperativeStarter> parent;

    protected CooperativeActor(final Actor<CooperativeActor> self, ArrayList<Long> output, Actor<CooperativeStarter> parent) {
        super(self);
        this.parent = parent;
        this.output = output;
        tx = new MockTransaction(10000000, 1000);
    }

    public void run() {
        Iterator<Long> answers = tx.query();
        if (answers.hasNext()) send(answers.next());
        self().tell((actor) -> actor.resume(answers));
    }

    private void send(final Long l) {
        output.add(l);
    }

    public void resume(Iterator<Long> r) {
        if (r.hasNext()) send(r.next());
        if (r.hasNext()) {
            self().tell((actor) -> actor.resume(r));
        } else {
            parent.tell((actor) -> actor.done());
        }
    }
}