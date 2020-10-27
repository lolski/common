package grakn.common.poc;

import grakn.common.concurrent.NamedThreadFactory;
import grakn.common.concurrent.actor.Actor;
import grakn.common.concurrent.actor.ActorRoot;
import grakn.common.concurrent.actor.eventloop.EventLoopSingleThreaded;

import java.util.ArrayList;

public class BackpressurePOC {

    public static void main(String[] args) throws InterruptedException {
        EventLoopSingleThreaded eventLoop = new EventLoopSingleThreaded(NamedThreadFactory.create(BackpressurePOC.class, "main"));
        ArrayList<Long> output = new ArrayList<>();
        Actor<ActorRoot> rootActor = Actor.root(eventLoop, ActorRoot::new);
        Actor<ControllerActor> controller = rootActor.ask(root -> root.<ControllerActor>createActor((self) -> new ControllerActor(self, output))).await();
        controller.tell((actor) -> actor.start());
    }
}

class ControllerActor extends Actor.State<ControllerActor> {

    ArrayList<Long> buffer;

    protected ControllerActor(final Actor<ControllerActor> self, final ArrayList<Long> output) {
        super(self);
    }

    public Actor<BackpressureActor> child() {
        return this.<BackpressureActor>child((actor) -> new BackpressureActor(actor, self()));
    }

    public void receive(Long l) {
        buffer.add(l);
    }
}

class BackpressureActor extends Actor.State<BackpressureActor> {

    private final Actor<ControllerActor> contoller;

    protected BackpressureActor(final Actor<BackpressureActor> self, Actor<ControllerActor> contoller) {
        super(self);
        this.contoller = contoller;
    }

    public Actor<BackpressureActor> child() {
        return this.<BackpressureActor>child((actor) -> new BackpressureActor(actor, contoller));
    }
}
