package grakn.common.poc.wip;

import grakn.common.concurrent.actor.Actor;
import grakn.common.concurrent.actor.ActorRoot;
import grakn.common.concurrent.actor.eventloop.EventLoopGroup;
import grakn.common.poc.reasoning.Registry;
import grakn.common.poc.reasoning.Atomic;
import grakn.common.poc.reasoning.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.LinkedBlockingQueue;

import static grakn.common.collection.Collections.list;
import static junit.framework.TestCase.assertTrue;

public class ExplanationTest {


    @Test
    public void explanationTest() throws InterruptedException {
        /*

        when {
            $x isa person, has age 10;
        }, then {
            $x has birth-year 2010;
        };

        query:
        match $x has birth-year 2010;

         */

        Registry registry = new Registry();

        LinkedBlockingQueue<Long> responses = new LinkedBlockingQueue<>();
        EventLoopGroup eventLoop = new EventLoopGroup(1, "reasoning-elg");
        Actor<ActorRoot> rootActor = Actor.root(eventLoop, ActorRoot::new);

        // create atomic actors first to control answer size
        registry.registerAtomic(10L, pattern ->
                rootActor.ask(actor ->
                        actor.<Atomic>createActor(self -> new Atomic(self, pattern, Arrays.asList(), 1L))
                ).awaitUnchecked()
        );
        registry.registerRule(list(10L), pattern ->
                rootActor.ask(actor ->
                        actor.<Rule>createActor(self -> new Rule(self, pattern, 0L))
                ).awaitUnchecked()
        );
        registry.registerAtomic(2010L, pattern ->
                rootActor.ask(actor ->
                        actor.<Atomic>createActor(self -> new Atomic(self, pattern, Arrays.asList(Arrays.asList(10L)), 0L))
                ).awaitUnchecked()
        );
//        Actor<ConjunctiveActor> conjunctive = rootActor.ask(actor ->
//                actor.<ConjunctiveActor>createActor(self -> new ConjunctiveActor(self, Arrays.asList(2010L), 0L, responses))
//        ).awaitUnchecked();

        long startTime = System.currentTimeMillis();
        long n = 0L + (1L) + 1; //total number of traversal answers, plus one expected DONE (-1 answer)
//        for (int i = 0; i < n; i++) {
//            conjunctive.tell(actor ->
//                    actor.executeReceiveRequest(
//                            new Request(new Plan(Arrays.asList(conjunctive)).toNextStep(), Arrays.asList(), Arrays.asList(), Arrays.asList()),
//                            actorRegistry
//                    )
//            );
//        }

        for (int i = 0; i < n - 1; i++) {
            Long answer = responses.take();
            assertTrue(answer != -1);
        }
    }
}
