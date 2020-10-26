package grakn.common.poc;

import grakn.common.concurrent.NamedThreadFactory;
import grakn.common.concurrent.actor.Actor;
import grakn.common.concurrent.actor.ActorRoot;
import grakn.common.concurrent.actor.eventloop.EventLoopSingleThreaded;

import java.util.ArrayList;
import java.util.Iterator;

public class ReasoningPOC {

    public static void main(String[] args) throws InterruptedException {
//        noncooperative();
//        cooperative();
        pong();
    }

    /*
    Use a cooperative actor with a starter to report when the cooperative actor has finished its task
     */
    public static void cooperative() throws InterruptedException {
        EventLoopSingleThreaded eventLoop = new EventLoopSingleThreaded(NamedThreadFactory.create(ReasoningPOC.class, "main"));
        ArrayList<Long> output = new ArrayList<>();
        Actor<ActorRoot> rootActor = Actor.root(eventLoop, ActorRoot::new);
        Actor<Starter> starter = rootActor.ask(root -> root.<Starter>createActor((self) -> new Starter(self, output))).await();
        // run a long task
        long start = System.currentTimeMillis();
        starter.tell((actor) -> actor.start());
        Thread.sleep(1000);
    }

    /*
    a noncooperative actor can be waited on to finish its single non-cooperative job
     */
    public static void noncooperative() throws InterruptedException {
        EventLoopSingleThreaded eventLoop = new EventLoopSingleThreaded(NamedThreadFactory.create(ReasoningPOC.class, "main"));
        ArrayList<Long> output = new ArrayList<>();
        Actor<ActorRoot> rootActor = Actor.root(eventLoop, ActorRoot::new);
//        Actor<NoncooperativeActor> noncooperativeActor = rootActor.ask(root -> root.<NoncooperativeActor>createActor((self) -> new NoncooperativeActor(self, output))).await();
//        // run a long task
//        long start = System.currentTimeMillis();
//        noncooperativeActor.order((actor) -> actor.run()).await();
//        long elapsed = System.currentTimeMillis() - start;
//        System.out.println("Time taken noncooperatively (ms): " + (elapsed));
//        System.out.println("Length of output array: " + (output.size()));
    }

    public static void pong() throws InterruptedException {
        EventLoopSingleThreaded eventLoop = new EventLoopSingleThreaded(NamedThreadFactory.create(ReasoningPOC.class, "main"));
        ArrayList<Long> output = new ArrayList<>();
        Actor<ActorRoot> rootActor = Actor.root(eventLoop, ActorRoot::new);
        Actor<RepeatNoncooperativeActor> starter = rootActor.ask(root -> root.<RepeatNoncooperativeActor>createActor((self) -> new RepeatNoncooperativeActor(self, output))).await();
        // run a long task
        long start = System.currentTimeMillis();
        starter.tell((actor) -> actor.start());
        Thread.sleep(10000);
    }
}

class RepeatNoncooperativeActor extends Actor.State<RepeatNoncooperativeActor> {
    private final Actor<NoncooperativeActor> noncooperativeActor;
    private long startTime;
    private ArrayList<Long> output;
    private int completed = 0;
    private int limit = 10000;

    protected RepeatNoncooperativeActor(final Actor<RepeatNoncooperativeActor> self, ArrayList<Long> output) {
        super(self);
        this.output = output;
        noncooperativeActor = this.<NoncooperativeActor>child((actor) -> new NoncooperativeActor(actor, output, self));
    }

    public void start() {
        this.startTime = System.currentTimeMillis();
        for (int i = 0; i < limit; i++) {
            noncooperativeActor.tell((actor) -> actor.run());
        }
    }

    public void done() {
        completed++;
        if (completed == limit) {
            long elapsed = System.currentTimeMillis() - startTime;
            System.out.println("Time taken noncooperatively for " + limit + " noncooperative repeats (ms): " + (elapsed));
            System.out.println("Length of output array: " + (output.size()));
        }
    }
}


class NoncooperativeActor extends Actor.State<NoncooperativeActor> {

    private final ArrayList<Long> output;
    private Actor<RepeatNoncooperativeActor> starter;
    MockTransaction tx;

    protected NoncooperativeActor(final Actor<NoncooperativeActor> self, ArrayList<Long> output, Actor<RepeatNoncooperativeActor> parent) {
        super(self);
        this.output = output;
        this.starter = parent;
        tx = new MockTransaction(1000, 100);
    }

    public void run() {
        long nanoStart = System.nanoTime();
        Iterator<Long> answers = tx.query();
        answers.forEachRemaining(l -> send(l));
        starter.tell((actor) -> actor.done());
        System.out.println("Time per run: " + (System.nanoTime() - nanoStart));
    }

    private void send(final Long l) {
        output.add(l);
    }
}

class Starter extends Actor.State<Starter> {
    private final Actor<CooperativeActor> cooperative;
    private long startTime;
    private ArrayList<Long> output;

    protected Starter(final Actor<Starter> self, ArrayList<Long> output) {
        super(self);
        this.output = output;
        cooperative = this.<CooperativeActor>child((actor) -> new CooperativeActor(actor, output, self));
    }

    public void start() {
        this.startTime = System.currentTimeMillis();
        cooperative.tell((actor) -> actor.begin());
    }

    public void done() {
        long elapsed = System.currentTimeMillis() - startTime;
        System.out.println("Time taken cooperatively (ms): " + (elapsed));
        System.out.println("Length of output array: " + (output.size()));
    }
}

class CooperativeActor extends Actor.State<CooperativeActor> {

    private final MockTransaction tx;
    private final ArrayList<Long> output;
    private final Actor<Starter> parent;

    protected CooperativeActor(final Actor<CooperativeActor> self, ArrayList<Long> output, Actor<Starter> parent) {
        super(self);
        this.parent = parent;
        this.output = output;
        tx = new MockTransaction(10000000, 1000);
    }

    public void begin() {
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

class MockTransaction {
    private long computeLength;
    private int answerInterval;

    public MockTransaction(long computeLength, int answerInterval) {
        this.computeLength = computeLength;
        this.answerInterval = answerInterval;
    }

    public Iterator<Long> query() {
        return new Iterator<Long>() {
            long count = 0L;

            @Override
            public boolean hasNext() {
                return count < computeLength;
            }

            @Override
            public Long next() {
                while (count < computeLength) {
                    if (count % answerInterval == 0) {
                        count++;
                        return count - 1;
                    } else {
                        count++;
                    }
                }
                return null;
            }
        };
    }
}


