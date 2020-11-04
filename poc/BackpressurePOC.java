package grakn.common.poc;

import grakn.common.concurrent.actor.Actor;
import grakn.common.concurrent.actor.ActorRoot;
import grakn.common.concurrent.actor.eventloop.EventLoopGroup;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static grakn.common.poc.BackpressurePOC.NUM_VALUES;
import static grakn.common.poc.BackpressurePOC.pressuredActors;


public class BackpressurePOC {

    static List<Actor<BackpressureActor>> pressuredActors;

    static int NUM_VALUES = 1000;

    public static void main(String[] args) throws InterruptedException {
        EventLoopGroup eventLoop = new EventLoopGroup(1, "reasoning-elg");
        ArrayList<Long> output = new ArrayList<>();
        Actor<ActorRoot> rootActor = Actor.root(eventLoop, ActorRoot::new);
        pressuredActors = new ArrayList<>();
        Actor<BackpressureActor> start = rootActor.ask(root -> root.<BackpressureActor>createActor((self) -> new BackpressureActor(self, null))).await();
        pressuredActors.add(start);
        for (int i = 0; i < 1000; i++) {
            Actor<BackpressureActor> child = pressuredActors.get(pressuredActors.size() - 1).ask((actor) -> actor.child()).await();
            pressuredActors.add(child);
        }

        Actor<BackpressureActor> last = pressuredActors.get(pressuredActors.size() - 1);
        int batch = 2;
        for (int i = 0; i < NUM_VALUES / batch; i++) {
            last.tell((actor) -> actor.pull(batch));
        }
        Thread.sleep(1000);
    }
}


class BackpressureActor extends Actor.State<BackpressureActor> {

    // set of actors that can be pulled from
    Set<Actor<BackpressureActor>> senders = new HashSet<>();
    // a queue per receiver of answers from this actor
    Map<Actor<BackpressureActor>, List<Runnable>> outbox = new HashMap<>();
    Map<Actor<BackpressureActor>, Integer> awaitingPush = new HashMap<>();
    // when a given outbox has less than this threshold, we initialise a new pull
    static int PULL_THRESHOLD = 2;

    List<Long> terminus = new ArrayList<>();

    // only for final or first actor
    MockTransaction tx;
    Iterator<Long> query;
    long startTime;

    protected BackpressureActor(final Actor<BackpressureActor> self, final Actor<BackpressureActor> prior) {
        super(self);
        if (prior == null) {
            tx = new MockTransaction(BackpressurePOC.NUM_VALUES, 1);
            query = tx.query();
        } else {
            senders.add(prior);
        }
    }

    public Actor<BackpressureActor> child() {
        return this.<BackpressureActor>child((actor) -> new BackpressureActor(actor, self()));
    }

    /*
    Another actor can pull answers from this actor
     */
    public void pull(Actor<BackpressureActor> receiver, int size) {
        outbox.putIfAbsent(receiver, new LinkedList<>());
        List<Runnable> receiverOutbox = outbox.get(receiver);
        awaitingPush.putIfAbsent(receiver, 0);

        if (receiverOutbox.size() < PULL_THRESHOLD) {
            pullFromSenders(Math.max(PULL_THRESHOLD, size));
            awaitingPush.compute(receiver, (key, value) -> value + size);
        } else {
            // TODO this UX can be improved, not obvious this is doing a tell()
            receiverOutbox.remove(0).run();
        }
    }

    // if you're the last actor, just pull from it
    public void pull(int size) {
        startTime = System.currentTimeMillis();
        if (terminus.size() < PULL_THRESHOLD) {
            pullFromSenders(size);
        } else {
            System.out.println("Got value 2: " + terminus.remove(0));
        }
    }

    private void pullFromSenders(int size) {
        if (senders.size() > 0) {
            for (Actor<BackpressureActor> sender : senders) {
                sender.tell((actor) -> actor.pull(self(), size));
            }
        } else {
            // send next value to all outbox
            int count = 0;
            while (query.hasNext() && count < size) {
                Long nextValue = query.next();
                for (Actor<BackpressureActor> receiver : outbox.keySet()) {
                    // top level message must visit all actors downstream
                    receiver.tell((actor) -> actor.partialAnswer(nextValue, self(), BackpressurePOC.pressuredActors.subList(2, pressuredActors.size())));
                }
                count++;
            }
        }
    }

    public void partialAnswer(Long l, Actor<BackpressureActor> sender, List<Actor<BackpressureActor>> toVisit) {
        senders.add(sender);
        if (toVisit.isEmpty()) {
            terminus.add(l);

//            System.out.println("Got value: " + l + ", elapsed: " + (System.currentTimeMillis() - startTime));
            if (terminus.size() == NUM_VALUES) {
                System.out.println("Got " + terminus.size() + " values in " + (System.currentTimeMillis() - startTime) + " milliseconds");
            }


        } else {
            Actor<BackpressureActor> next = toVisit.get(0);
            ArrayList<Actor<BackpressureActor>> nextToVisit = new ArrayList<>(toVisit.subList(1, toVisit.size()));
            push(l, nextToVisit, next);
        }
    }

    private void push(Long l, List<Actor<BackpressureActor>> nextToVisit, Actor<BackpressureActor> receiver) {
        // pass it through into the outbox if there is no active push request
        Integer receiverAwaiting = awaitingPush.get(receiver);
        if (receiverAwaiting > 0) {
            receiver.tell((actor) -> actor.partialAnswer(l, self(), nextToVisit));
            awaitingPush.compute(receiver, (key, value) -> value - 1);
        } else {
            outbox.get(receiver).add(() -> receiver.tell((actor) -> actor.partialAnswer(l, self(), nextToVisit)));
        }
    }
}
