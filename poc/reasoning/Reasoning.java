package grakn.common.poc.reasoning;

import grakn.common.concurrent.actor.Actor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class Reasoning {
    public static void main(String[] args) {


    }
}

class AtomicActor extends Actor.State<AtomicActor> {

    private final Map<Request, ResponseProducer> responseProducers;
    private final Long queryPattern;

    public AtomicActor(final Actor<AtomicActor> self, Long queryPattern) {
        super(self);
        this.queryPattern = queryPattern;
        // the query pattern long represents some thing to pass to a traversal or resolve further
        responseProducers = new HashMap<>();
    }

    public void receiveRequest(Request request) {
        if (!responseProducers.containsKey(request)) responseProducers.put(request, createResponseProducer(request));

        ResponseProducer responseProducer = responseProducers.get(request);
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
        ResponseProducer responseProducer = responseProducers.get(request);

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
        if (responseProducer.traversalProducer.hasNext()) {
            // TODO could batch traverse here
            Long answer = responseProducer.traversalProducer.next();
            answer += this.queryPattern;
            return Arrays.asList(answer);
        }
        return Arrays.asList();
    }

    private void dispatchResponseAnswers(final Request request, final ResponseProducer responseProducer) {
        // send as many answers as possible to requester
        for (int i = 0; i < Math.min(responseProducer.requested, responseProducer.answers.size()); i++) {
            Long answer = responseProducer.answers.remove(0);
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

    private void dispatchResponseDone(final Request request) {
        Actor<AtomicActor> requester = request.returnPath.get(request.returnPath.size() - 1);
        ResponseDone responseDone = new ResponseDone(request);
        requester.tell((actor) -> actor.receiveDone(responseDone));
    }


    private ResponseProducer createResponseProducer(final Request request) {
        // initialise traversal
        Iterator<Long> traversal = (new MockTransaction(10, 1)).query();
        // initialise downstream producers
        return new ResponseProducer(traversal);
    }
}

class ResponseProducer {
    Iterator<Long> traversalProducer;
    List<Long> answers = new LinkedList<>();
    int requested = 0;
    int dispatched = 0;

    public ResponseProducer(Iterator<Long> traversalProducer) {
        this.traversalProducer = traversalProducer;
    }

    public boolean finished() {
        return !traversalProducer.hasNext();
    }
}


interface Response {
    Request request();
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

