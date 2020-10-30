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

import static junit.framework.TestCase.assertTrue;

public class AtomicActor extends Actor.State<AtomicActor> {

    private final Map<Request, ResponseProducer> requestProducers;
    private final Map<Request, Request> requestRouter; // TODO note that this can be many to one, and is not catered for yet (ie. request followed the same request)
    private final String name;
    private final Long traversalPattern;
    private final long traversalIteratorLength;
    @Nullable private final LinkedBlockingQueue<Long> responses;

    public AtomicActor(final Actor<AtomicActor> self, final String name, Long traversalPattern, final long traversalIteratorLength, final @Nullable LinkedBlockingQueue<Long> responses) {
        super(self);
        this.name = name;
        this.traversalPattern = traversalPattern;
        this.traversalIteratorLength = traversalIteratorLength;
        this.responses = responses;
        // the query pattern long represents some thing to pass to a traversal or resolve further
        requestProducers = new HashMap<>();
        requestRouter = new HashMap<>();
    }

    public void receiveRequest(final Request request) {
        System.out.println("Received request in: " + name);
        if (!this.requestProducers.containsKey(request)) {
            this.requestProducers.put(request, initialiseResponseProducer(request));
        }

        ResponseProducer responseProducer = this.requestProducers.get(request);

        if (responseProducer.finished()) respondDoneToRequester(request);
        else {
            // TODO if we want batching, we increment by as many as are requested
            responseProducer.requestsFromUpstream++;

            if (responseProducer.requestsFromUpstream > responseProducer.requestsToDownstream + responseProducer.answers.size()) {
                List<Long> answers = produceTraversalAnswers(responseProducer);
                responseProducer.answers.addAll(answers);
                respondAnswersToRequester(request, responseProducer);
            }

            if (responseProducer.requestsFromUpstream > responseProducer.requestsToDownstream + responseProducer.answers.size()) {
                if (responseProducer.downstream != null) {
                    requestFromDownstream(request, responseProducer);
                }
            }
        }
    }

    public void receiveAnswer(final ResponseAnswer answer) {
        System.out.println("Received answer response in: " + name);
        assertTrue(responses.isEmpty());
        Request sourceSubRequest = answer.request();
        Request parentRequest = requestRouter.get(sourceSubRequest);
        ResponseProducer responseProducer = requestProducers.get(parentRequest);

        List<Long> partialAnswers = answer.partialAnswers;
        Long mergedAnswers = partialAnswers.stream().reduce(0L, (acc, v) -> acc + v);
        registerTraversal(responseProducer, mergedAnswers);

        List<Long> answers = produceTraversalAnswers(responseProducer);
        responseProducer.answers.addAll(answers);
        respondAnswersToRequester(parentRequest, responseProducer);
    }

    public void receiveDone(final ResponseDone done) {
        System.out.println("Received done response in: " + name);
        Request sourceSubRequest = done.request();
        Request parentRequest = requestRouter.get(sourceSubRequest);
        ResponseProducer responseProducer = requestProducers.get(parentRequest);

        Actor<AtomicActor> responder = done.responder();
        responseProducer.downstreamDone(responder);

        List<Long> answers = produceTraversalAnswers(responseProducer);
        responseProducer.answers.addAll(answers);
        respondAnswersToRequester(parentRequest, responseProducer);

        if (responseProducer.finished()) {
            respondDoneToRequester(parentRequest);
        }
        /*
        TODO: major flaw here is that when we get a DONE, we have fewer messages dispatched which should have
        TODO: lead to answers to the original request. To compensate, we should "retry" getting answers
        TODO: either from another actor, or a local traversal from the list of local traversals
         */
    }

    private void requestFromDownstream(final Request request, final ResponseProducer responseProducer) {
        Actor<AtomicActor> downstream = responseProducer.downstream;
        Routing newRouting = request.routing.copy();
        newRouting.extendResponsePath(self());
        newRouting.trimRequestPath();
        Request subrequest = new Request(
                newRouting,
                request.partialAnswers,
                request.constraints,
                request.unifiers
        );

        // TODO we may overwrite if multiple identical requests are sent
        requestRouter.put(subrequest, request);

        System.out.println("Requesting from downstream from: " + name);
        downstream.tell(actor -> actor.receiveRequest(subrequest));
    }

    private void respondAnswersToRequester(final Request request, final ResponseProducer responseProducer) {
        // send as many answers as possible to requester
        for (int i = 0; i < Math.min(responseProducer.requestsFromUpstream, responseProducer.answers.size()); i++) {
            Long answer = responseProducer.answers.remove(0);
            if (request.routing.responsePath.isEmpty()) {
                // base case - how to return from Actor model
                assert responses != null : this + ": can't return answers because the user answers queue is null";
                System.out.println("Saving answer to output queue in actor: " + name);
                responses.add(answer);
            } else {
                Actor<AtomicActor> requester = request.routing.getRequester();
                Routing newRouting = request.routing.copy();
                newRouting.trimResponsePath();
                List<Long> newAnswers = new ArrayList<>(1 + request.partialAnswers.size());
                newAnswers.addAll(request.partialAnswers);
                newAnswers.add(answer);
                ResponseAnswer responseAnswer = new ResponseAnswer(
                        request,
                        self(),
                        newRouting,
                        newAnswers,
                        request.constraints,
                        request.unifiers
                );

                System.out.println("Responding answer to requester from actor: " + name);
                requester.tell((actor) -> actor.receiveAnswer(responseAnswer));
            }
            responseProducer.requestsFromUpstream--;
        }
    }

    private void respondDoneToRequester(final Request request) {
        if (request.routing.responsePath.isEmpty()) {
            // base case - how to return from Actor model
            assert responses != null : this + ": can't return answers because the user answers queue is null";
            System.out.println("Sending DONE response to output from actor: " + name);
            responses.add(-1L);
        } else {
            Actor<AtomicActor> requester = request.routing.getRequester();
            ResponseDone responseDone = new ResponseDone(request, self());
            System.out.println("Responding Done to requester from actor: " + name);
            requester.tell((actor) -> actor.receiveDone(responseDone));
        }
    }

    private List<Long> produceTraversalAnswers(final ResponseProducer responseProducer) {
        Iterator<Long> traversalProducer = responseProducer.getOneTraversalProducer();
        if (traversalProducer != null) {
            // TODO could do batch traverse, or retrieve answers from multiple traversals
            Long answer = traversalProducer.next();
            if (!traversalProducer.hasNext()) responseProducer.removeTraversalProducer(traversalProducer);
            answer += this.traversalPattern;
            return Arrays.asList(answer);
        }
        return Arrays.asList();
    }

    private ResponseProducer initialiseResponseProducer(final Request request) {
        Actor<AtomicActor> downstream = request.routing.downstream();
        ResponseProducer responseProducer = new ResponseProducer(downstream);

        if (!responseProducer.hasDownstream()) {
            registerTraversal(responseProducer, 0L);
        }

        return responseProducer;
    }

    private void registerTraversal(ResponseProducer responseProducer, long partialAnswer) {
        Iterator<Long> traversal = (new MockTransaction(traversalIteratorLength, 1)).query(partialAnswer);
        responseProducer.addTraversalProducer(traversal);
    }
}

class ResponseProducer {
    List<Iterator<Long>> traversalProducers;
    @Nullable Actor<AtomicActor> downstream = null; // null if there is no downstream or if downstream exhausted
    List<Long> answers = new LinkedList<>();
    int requestsFromUpstream = 0;
    int requestsToDownstream = 0;

    public ResponseProducer(@Nullable Actor<AtomicActor> downstream) {
        this.traversalProducers = new ArrayList<>();
        this.downstream = downstream;
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

    public boolean hasDownstream() {
        return downstream != null;
    }

    public boolean finished() {
        return downstream == null && traversalProducers.isEmpty();
    }

    public void downstreamDone(final Actor<AtomicActor> responder) {
        downstream = null;
    }
}


class Request {
    final Routing routing;
    final List<Long> partialAnswers;
    final List<Object> constraints;
    final List<Object> unifiers;

    public Request(Routing routing,
                   List<Long> partialAnswers,
                   List<Object> constraints,
                   List<Object> unifiers) {
        this.routing = routing;
        this.partialAnswers = partialAnswers;
        this.constraints = constraints;
        this.unifiers = unifiers;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Request request = (Request) o;
        return Objects.equals(routing, request.routing) &&
                Objects.equals(partialAnswers, request.partialAnswers) &&
                Objects.equals(constraints, request.constraints) &&
                Objects.equals(unifiers, request.unifiers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(routing, partialAnswers, constraints, unifiers);
    }
}


interface Response {
    Request request();
    Actor<AtomicActor> responder();
}

class ResponseDone implements Response {
    private final Request request;
    private final Actor<AtomicActor> responder;

    public ResponseDone(final Request request, final Actor<AtomicActor> responder) {
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
    final Routing routing;
    final List<Long> partialAnswers;
    final List<Object> constraints;
    final List<Object> unifiers;
    private final Request request;
    private final Actor<AtomicActor> responder;

    public ResponseAnswer(final Request request,
                          final Actor<AtomicActor> responder,
                          final Routing routing,
                          final List<Long> partialAnswers,
                          final List<Object> constraints,
                          final List<Object> unifiers) {
        assert routing.responsePath.size() == 0 : "response answer must have an empty responsePath";
        this.request = request;
        this.responder = responder;
        this.routing = routing;
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

class Routing {
    List<Actor<AtomicActor>> responsePath;
    List<Actor<AtomicActor>> requestPath;

    public Routing(List<Actor<AtomicActor>> responsePath, List<Actor<AtomicActor>> requestPath) {
        this.responsePath = responsePath;
        this.requestPath = requestPath;
    }

    public Routing copy() {
        return new Routing(new ArrayList<>(responsePath), new ArrayList<>(requestPath));
    }

    public void trimRequestPath() {
        assert requestPath.size() >= 1 : "can't trim a path object that's already empty";
        requestPath.remove(requestPath.size()-1);
    }

    public void extendResponsePath(Actor<AtomicActor> respondTo) {
        responsePath.add(respondTo);
    }

    public void trimResponsePath() {
        assert responsePath.size() >= 1 : "can't trim a path object that's already empty";
        responsePath.remove(responsePath.size()-1);
    }

    @Nullable public Actor<AtomicActor> downstream() {
        if (requestPath.size() >= 2) {
            return requestPath.get(requestPath.size()-2);
        }
        return null;
    }

    public Actor<AtomicActor> getRequester() {
        assert responsePath.size() >= 1 : "can't get the last element of responsePath as it's empty";
        return responsePath.get(responsePath.size() - 1);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Routing routing = (Routing) o;
        return Objects.equals(responsePath, routing.responsePath) &&
                Objects.equals(requestPath, routing.requestPath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(responsePath, requestPath);
    }
}