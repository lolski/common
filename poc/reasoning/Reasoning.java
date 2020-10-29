package grakn.common.poc.reasoning;

import grakn.common.concurrent.NamedThreadFactory;
import grakn.common.concurrent.actor.Actor;
import grakn.common.concurrent.actor.ActorRoot;
import grakn.common.concurrent.actor.eventloop.EventLoopSingleThreaded;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;

public class Reasoning {
    public static LinkedBlockingQueue<Long> answers = new LinkedBlockingQueue<>();

    public static void main(String[] args) throws InterruptedException {
        EventLoopSingleThreaded eventLoop = new EventLoopSingleThreaded(NamedThreadFactory.create(Reasoning.class, "main"));
        Actor<ActorRoot> rootActor = Actor.root(eventLoop, ActorRoot::new);
        Actor<AtomicActor> atomic = rootActor.ask(root -> root.<AtomicActor>createActor((self) -> new AtomicActor(self, 1L))).await();
        atomic.tell(actor ->
                actor.receiveRequest(
                        new Request(Arrays.asList(), Arrays.asList(), Arrays.asList(), Arrays.asList(), Arrays.asList())
                )
        );
        atomic.tell(actor ->
                actor.receiveRequest(
                        new Request(Arrays.asList(), Arrays.asList(), Arrays.asList(), Arrays.asList(), Arrays.asList())
                )
        );
        System.out.println(answers.take());
        System.out.println(answers.take());
        System.out.println(answers.take());
        System.out.println("should not be printed");
    }
}

class AtomicActor extends Actor.State<AtomicActor> {

    private final Map<Request, ResponseProducer> request;
    private final Map<Request, Request> subrequest;
    private final Long queryPattern;

    public AtomicActor(final Actor<AtomicActor> self, Long queryPattern) {
        super(self);
        this.queryPattern = queryPattern;
        // the query pattern long represents some thing to pass to a traversal or resolve further
        request = new HashMap<>();
        subrequest = new HashMap<>();
    }

    public void receiveRequest(Request request) {
        if (!this.request.containsKey(request)) {
            this.request.put(request, createResponseProducer(request));
        }

        ResponseProducer responseProducer = this.request.get(request);
        // TODO if we want batching, we increment by as many as are requested
        responseProducer.requested++;

        if (responseProducer.requested > responseProducer.dispatched + responseProducer.answers.size()) {
            List<Long> answers = produceTraversalAnswers(responseProducer);
            enqueueAnswers(request, responseProducer, answers);
        }

        if (responseProducer.finished()) dispatchResponseDone(request);
    }

    public void receiveDone(ResponseDone done) {
        Request request = done.request();
        ResponseProducer responseProducer = this.request.get(request);

        if (responseProducer.finished()) {
            dispatchResponseDone(request);
        }
    }

    public void receiveAnswer(ResponseAnswer answer) {

    }

    private void enqueueAnswers(final Request request, final ResponseProducer responseProducer, final List<Long> answers) {
        responseProducer.answers.addAll(answers);
        dispatchResponseAnswers(request, responseProducer);
    }

    private List<Long> produceTraversalAnswers(final ResponseProducer responseProducer) {
        if (!responseProducer.traversalProducers.isEmpty() && responseProducer.traversalProducers.get(0).hasNext()) {
            // TODO could do batch traverse, or retrieve answers from multiple traversals
            Long answer = responseProducer.traversalProducers.get(0).next();
            answer += this.queryPattern;
            return Arrays.asList(answer);
        }
        return Arrays.asList();
    }

    private void dispatchResponseAnswers(final Request request, final ResponseProducer responseProducer) {
        // send as many answers as possible to requester
        for (int i = 0; i < Math.min(responseProducer.requested, responseProducer.answers.size()); i++) {
            Long answer = responseProducer.answers.remove(0);
            if (request.returnPath.isEmpty()) {
                // base case - how to return from Actor model
                Reasoning.answers.add(answer);
            } else {
                Actor<AtomicActor> requester = request.returnPath.get(request.returnPath.size() - 1);

                List<Long> newAnswers = new ArrayList<>(1 + request.partialAnswers.size());
                newAnswers.addAll(request.partialAnswers);
                newAnswers.add(answer);
                ResponseAnswer responseAnswer = new ResponseAnswer(
                        request,
                        new ArrayList<>(request.returnPath.subList(0, request.returnPath.size() - 1)),
                        request.gotoPath,
                        newAnswers,
                        request.constraints,
                        request.unifiers
                );

                requester.tell((actor) -> actor.receiveAnswer(responseAnswer));
            }
        }
    }

    private void dispatchResponseDone(final Request request) {
        Actor<AtomicActor> requester = request.returnPath.get(request.returnPath.size() - 1);
        ResponseDone responseDone = new ResponseDone(request);
        requester.tell((actor) -> actor.receiveDone(responseDone));
    }


    private ResponseProducer createResponseProducer(final Request request) {
        Actor<AtomicActor> dependency = null;
        if (request.gotoPath.size() >= 2) {
            dependency = request.gotoPath.get(request.gotoPath.size() - 2);
        }

        ResponseProducer responseProducer = new ResponseProducer(dependency);
        if (!responseProducer.hasDependency()) {
            Iterator<Long> traversal = (new MockTransaction(10, 1)).query();
            responseProducer.addTraversalProducer(traversal);
        }

        return responseProducer;
    }
}

class ResponseProducer {
    List<Iterator<Long>> traversalProducers;
    @Nullable Actor<AtomicActor> dependency = null; // null if there is no dependency or if dependency exhausted
    List<Long> answers = new LinkedList<>();
    int requested = 0;
    int dispatched = 0;

    public ResponseProducer(@Nullable Actor<AtomicActor> dependency) {
        this.traversalProducers = new ArrayList<>();
        this.dependency = dependency;
    }

    public void addTraversalProducer(Iterator<Long> traversalProducer) {
        traversalProducers.add(traversalProducer);
    }

    public void removeTraversalProducer(Iterator<Long> traversalProducer) {
        traversalProducers.remove(traversalProducer);
    }

    public boolean hasDependency() {
        return dependency != null;
    }

    public boolean finished() {
        return dependency == null && traversalProducers.isEmpty();
    }
}

class Request {
    final List<Actor<AtomicActor>> returnPath;
    final List<Actor<AtomicActor>> gotoPath;
    final List<Long> partialAnswers;
    final List<Object> constraints;
    final List<Object> unifiers;

    public Request(List<Actor<AtomicActor>> returnPath,
                   List<Actor<AtomicActor>> gotoPath,
                   List<Long> partialAnswers,
                   List<Object> constraints,
                   List<Object> unifiers) {

        this.returnPath = returnPath;
        this.gotoPath = gotoPath;
        this.partialAnswers = partialAnswers;
        this.constraints = constraints;
        this.unifiers = unifiers;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Request request = (Request) o;
        return Objects.equals(returnPath, request.returnPath) &&
                Objects.equals(gotoPath, request.gotoPath) &&
                Objects.equals(partialAnswers, request.partialAnswers) &&
                Objects.equals(constraints, request.constraints) &&
                Objects.equals(unifiers, request.unifiers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(returnPath, gotoPath, partialAnswers, constraints, unifiers);
    }
}

class Response {
    
}

class ResponseDone implements Response {
    private Request request;

    public ResponseDone(Request request) {
        this.request = request;
    }

    @Override
    public Request request() {
        return request;
    }
}

class ResponseAnswer implements Response {
    private final Request request;
    private final List<Actor<AtomicActor>> returnPath;
    private final List<Actor<AtomicActor>> gotoPath;
    private final List<Long> partialAnswers;
    private final List<Object> constraints;
    private final List<Object> unifiers;

    public ResponseAnswer(Request request,
                          List<Actor<AtomicActor>> returnPath,
                          List<Actor<AtomicActor>> gotoPath,
                          List<Long> partialAnswers,
                          List<Object> constraints,
                          List<Object> unifiers) {

        this.request = request;
        this.returnPath = returnPath;
        this.gotoPath = gotoPath;
        this.partialAnswers = partialAnswers;
        this.constraints = constraints;
        this.unifiers = unifiers;
    }

    @Override
    public Request request() {
        return request;
    }
}

