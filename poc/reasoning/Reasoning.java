package grakn.common.poc.reasoning;

import grakn.common.concurrent.actor.Actor;

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
}

class AtomicActor extends Actor.State<AtomicActor> {

    private final Map<Request, ResponseProducer> requests;
    private final Map<Request, Request> subrequests; // TODO note that this can be many to one, and is not catered for yet (ie. request followed the same request)
    private final Long queryPattern;
    private final long txIteratorLength;
    @Nullable private final LinkedBlockingQueue<Long> answers;

    public AtomicActor(final Actor<AtomicActor> self, Long queryPattern, long txIteratorLength, @Nullable LinkedBlockingQueue<Long> answers) {
        super(self);
        this.queryPattern = queryPattern;
        this.txIteratorLength = txIteratorLength;
        this.answers = answers;
        // the query pattern long represents some thing to pass to a traversal or resolve further
        requests = new HashMap<>();
        subrequests = new HashMap<>();
    }

    public void receiveRequest(Request request) {
        if (!this.requests.containsKey(request)) {
            this.requests.put(request, createResponseProducer(request));
        }

        ResponseProducer responseProducer = this.requests.get(request);
        // TODO if we want batching, we increment by as many as are requested
        responseProducer.requested++;

        if (responseProducer.requested > responseProducer.dispatched + responseProducer.answers.size()) {
            List<Long> answers = produceTraversalAnswers(responseProducer);
            enqueueAnswers(request, responseProducer, answers);
        }

        if (responseProducer.requested > responseProducer.dispatched + responseProducer.answers.size()) {
            if (responseProducer.dependency != null) {
                requestFromDependency(request, responseProducer);
            }
        }

        if (responseProducer.finished()) dispatchResponseDone(request);
    }

    private void requestFromDependency(final Request request, final ResponseProducer responseProducer) {
        Actor<AtomicActor> dependency = responseProducer.dependency;
        List<Actor<AtomicActor>> returnPath = new ArrayList<>(request.returnPath);
        returnPath.add(self());
        Request subrequest = new Request(
                returnPath,
                new ArrayList<>(request.gotoPath.subList(0, request.gotoPath.size() - 1)),
                request.partialAnswers,
                request.constraints,
                request.unifiers
        );

        // TODO we may overwrite if multiple identical requests are sent
        subrequests.put(subrequest, request);

        dependency.tell(actor -> actor.receiveRequest(subrequest));
    }

    public void receiveDone(ResponseDone done) {
        Request sourceSubRequest = done.request();
        Request parentRequest = subrequests.get(sourceSubRequest);
        ResponseProducer responseProducer = requests.get(parentRequest);

        Actor<AtomicActor> responder = done.responder();
        responseProducer.dependencyDone(responder);

        if (responseProducer.finished()) {
            dispatchResponseDone(parentRequest);
        }


        /*
        TODO: major flaw here is that when we get a DONE, we have fewer messages dispatched which should have
        TODO: lead to answers to the original request. To compensate, we should "retry" getting answers
        TODO: either from another actor, or a local traversal from the list of local traversals
         */
    }

    public void receiveAnswer(ResponseAnswer answer) {
        Request sourceSubRequest = answer.request();
        Request parentRequest = subrequests.get(sourceSubRequest);
        ResponseProducer responseProducer = requests.get(parentRequest);

        List<Long> partialAnswers = answer.partialAnswers;
        Long mergedAnswers = partialAnswers.stream().reduce(0L, (acc, v) -> acc + v);

        Iterator<Long> traversal = (new MockTransaction(txIteratorLength, 1)).query(mergedAnswers);
        responseProducer.addTraversalProducer(traversal);

        List<Long> answers = produceTraversalAnswers(responseProducer);
        enqueueAnswers(parentRequest, responseProducer, answers);
    }

    private void enqueueAnswers(final Request request, final ResponseProducer responseProducer, final List<Long> answers) {
//        System.out.println(answers);
        responseProducer.answers.addAll(answers);
        dispatchResponseAnswers(request, responseProducer);
    }

    private List<Long> produceTraversalAnswers(final ResponseProducer responseProducer) {
        Iterator<Long> traversalProducer = responseProducer.getOneTraversalProducer();
        if (traversalProducer != null) {
            // TODO could do batch traverse, or retrieve answers from multiple traversals
            Long answer = traversalProducer.next();
            if (!traversalProducer.hasNext()) responseProducer.removeTraversalProducer(traversalProducer);
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
                assert answers != null : this + ": can't return answers because the user answers queue is null";
                answers.add(answer);
            } else {
                Actor<AtomicActor> requester = request.returnPath.get(request.returnPath.size() - 1);

                List<Long> newAnswers = new ArrayList<>(1 + request.partialAnswers.size());
                newAnswers.addAll(request.partialAnswers);
                newAnswers.add(answer);
                ResponseAnswer responseAnswer = new ResponseAnswer(
                        request,
                        self(),
                        new ArrayList<>(request.returnPath.subList(0, request.returnPath.size() - 1)),
                        newAnswers,
                        request.constraints,
                        request.unifiers
                );

                requester.tell((actor) -> actor.receiveAnswer(responseAnswer));
            }
            responseProducer.requested--;
        }
    }

    private void dispatchResponseDone(final Request request) {
        Actor<AtomicActor> requester = request.returnPath.get(request.returnPath.size() - 1);
        ResponseDone responseDone = new ResponseDone(request, self());
        requester.tell((actor) -> actor.receiveDone(responseDone));
    }


    private ResponseProducer createResponseProducer(final Request request) {
        Actor<AtomicActor> dependency = null;
        if (request.gotoPath.size() >= 2) {
            dependency = request.gotoPath.get(request.gotoPath.size() - 2);
        }

        ResponseProducer responseProducer = new ResponseProducer(dependency);
        if (!responseProducer.hasDependency()) {
            Iterator<Long> traversal = (new MockTransaction(txIteratorLength, 1)).query(0L);
            responseProducer.addTraversalProducer(traversal);
        }

        return responseProducer;
    }
}

class ResponseProducer {
    List<Iterator<Long>> traversalProducers;
    @Nullable
    Actor<AtomicActor> dependency = null; // null if there is no dependency or if dependency exhausted
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

    @Nullable
    public Iterator<Long> getOneTraversalProducer() {
        if (!traversalProducers.isEmpty()) return traversalProducers.get(0);
        return null;
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

    public void dependencyDone(final Actor<AtomicActor> responder) {
        assert dependency == responder;
        dependency = null;
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


interface Response {
    Request request();
    Actor<AtomicActor> responder();
}

class ResponseDone implements Response {
    private Request request;
    private Actor<AtomicActor> responder;

    public ResponseDone(Request request, Actor<AtomicActor> responder) {
        this.request = request;
        this.responder = responder;
    }

    @Override
    public Request request() {
        return request;
    }

    @Override
    public Actor<AtomicActor> responder() {
        return responder;
    }
}

class ResponseAnswer implements Response {
    private final Request request;
    private Actor<AtomicActor> responder;
    final List<Actor<AtomicActor>> returnPath;
    final List<Long> partialAnswers;
    final List<Object> constraints;
    final List<Object> unifiers;

    public ResponseAnswer(Request request,
                          Actor<AtomicActor> responder,
                          List<Actor<AtomicActor>> returnPath,
                          List<Long> partialAnswers,
                          List<Object> constraints,
                          List<Object> unifiers) {
        this.request = request;
        this.responder = responder;
        this.returnPath = returnPath;
        this.partialAnswers = partialAnswers;
        this.constraints = constraints;
        this.unifiers = unifiers;
    }

    @Override
    public Request request() {
        return request;
    }

    @Override
    public Actor<AtomicActor> responder() {
        return responder;
    }
}

