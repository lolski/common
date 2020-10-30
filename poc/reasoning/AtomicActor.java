package grakn.common.poc.reasoning;

import grakn.common.concurrent.actor.Actor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

public class AtomicActor extends Actor.State<AtomicActor> {
    Logger LOG;

    private final Map<Request, ResponseProducer> requestProducers;
    private final Map<Request, Request> requestRouter; // TODO note that this can be many to one, and is not catered for yet (ie. request followed the same request)
    private final String name;
    private final Long traversalPattern;
    private final long traversalIteratorLength;
    @Nullable private final LinkedBlockingQueue<Long> responses;

    public AtomicActor(final Actor<AtomicActor> self, final String name, Long traversalPattern, final long traversalIteratorLength, final @Nullable LinkedBlockingQueue<Long> responses) {
        super(self);
        LOG = LoggerFactory.getLogger(AtomicActor.class + "-" + name);
        this.name = name;
        this.traversalPattern = traversalPattern;
        this.traversalIteratorLength = traversalIteratorLength;
        this.responses = responses;
        // the query pattern long represents some thing to pass to a traversal or resolve further
        requestProducers = new HashMap<>();
        requestRouter = new HashMap<>();
    }

    public void receiveRequest(final Request request) {
        LOG.debug("Received request in: " + name);
        if (!this.requestProducers.containsKey(request)) {
            this.requestProducers.put(request, initialiseResponseProducer(request));
        }

        ResponseProducer responseProducer = this.requestProducers.get(request);

        if (responseProducer.finished()) {
            respondDoneToRequester(request);
        } else {
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

    /*
    When a receive and answer and pass the answer forward
    We map the request that generated the answer, to the originating request.
    We then copy the originating request, and clear the request path, as it must already have been satisfied.
     */
    public void receiveAnswer(final ResponseAnswer answer) {
        LOG.debug("Received answer response in: " + name);
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
        LOG.debug("Received done response in: " + name);
        Request sourceSubRequest = done.request();
        Request parentRequest = requestRouter.get(sourceSubRequest);
        ResponseProducer responseProducer = requestProducers.get(parentRequest);

        responseProducer.downstreamDone();

        if (responseProducer.finished()) {
            respondDoneToRequester(parentRequest);
        } else {
            List<Long> answers = produceTraversalAnswers(responseProducer);
            responseProducer.answers.addAll(answers);
            respondAnswersToRequester(parentRequest, responseProducer);
        }



        /*
        TODO: major flaw here is that when we get a DONE, we have fewer messages dispatched which should have
        TODO: lead to answers to the original request. To compensate, we should "retry" getting answers
        TODO: either from another actor, or a local traversal from the list of local traversals
         */
    }

    private void requestFromDownstream(final Request request, final ResponseProducer responseProducer) {
        Actor<AtomicActor> downstream = responseProducer.downstream;
        Path downstreamPath = request.path.moveDownstream();
        Request subrequest = new Request(
                downstreamPath,
                request.partialAnswers,
                request.constraints,
                request.unifiers
        );

        // TODO we may overwrite if multiple identical requests are sent
        requestRouter.put(subrequest, request);

        LOG.debug("Requesting from downstream from: " + name);
        downstream.tell(actor -> actor.receiveRequest(subrequest));
    }

    /*
    when we respond to a request with an answer, must trim requestPath
     */
    private void respondAnswersToRequester(final Request request, final ResponseProducer responseProducer) {
        // send as many answers as possible to requester
        for (int i = 0; i < Math.min(responseProducer.requestsFromUpstream, responseProducer.answers.size()); i++) {
            Long answer = responseProducer.answers.remove(0);
            if (request.path.atRoot()) {
                // base case - how to return from Actor model
                assert responses != null : this + ": can't return answers because the user answers queue is null";
                LOG.debug("Saving answer to output queue in actor: " + name);
                responses.add(answer);
            } else {
                Actor<AtomicActor> requester = request.path.directUpstream();
                Path newPath = request.path.moveUpstream();
                List<Long> newAnswers = new ArrayList<>(1 + request.partialAnswers.size());
                newAnswers.addAll(request.partialAnswers);
                newAnswers.add(answer);
                ResponseAnswer responseAnswer = new ResponseAnswer(
                        request,
                        newPath,
                        newAnswers,
                        request.constraints,
                        request.unifiers
                );

                LOG.debug("Responding answer to requester from actor: " + name);
                requester.tell((actor) -> actor.receiveAnswer(responseAnswer));
            }
            responseProducer.requestsFromUpstream--;
        }
    }

    private void respondDoneToRequester(final Request request) {
        if (request.path.atRoot()) {
            // base case - how to return from Actor model
            assert responses != null : this + ": can't return answers because the user answers queue is null";
            LOG.debug("Sending DONE response to output from actor: " + name);
            responses.add(-1L);
        } else {
            Actor<AtomicActor> requester = request.path.directUpstream();
            Path newPath = request.path.moveUpstream();
            ResponseDone responseDone = new ResponseDone(request, newPath);
            LOG.debug("Responding Done to requester from actor: " + name);
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
        Actor<AtomicActor> downstream = request.path.directDownstream();
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

    public void downstreamDone() {
        downstream = null;
    }
}


class Request {
    final Path path;
    final List<Long> partialAnswers;
    final List<Object> constraints;
    final List<Object> unifiers;

    public Request(Path path,
                   List<Long> partialAnswers,
                   List<Object> constraints,
                   List<Object> unifiers) {
        this.path = path;
        this.partialAnswers = partialAnswers;
        this.constraints = constraints;
        this.unifiers = unifiers;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Request request = (Request) o;
        return Objects.equals(path, request.path) &&
                Objects.equals(partialAnswers, request.partialAnswers) &&
                Objects.equals(constraints, request.constraints) &&
                Objects.equals(unifiers, request.unifiers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, partialAnswers, constraints, unifiers);
    }
}


interface Response {
    Request request();
}

class ResponseDone implements Response {
    private final Request request;
    final Path path;

    public ResponseDone(final Request request, final Path path) {
        this.request = request;
        this.path = path;
    }

    @Override
    public Request request() {
        return request;
    }

}

class ResponseAnswer implements Response {
    final Path path;
    final List<Long> partialAnswers;
    final List<Object> constraints;
    final List<Object> unifiers;
    private final Request request;

    public ResponseAnswer(final Request request,
                          final Path path,
                          final List<Long> partialAnswers,
                          final List<Object> constraints,
                          final List<Object> unifiers) {
        this.request = request;
        this.path = path;
        this.partialAnswers = partialAnswers;
        this.constraints = constraints;
        this.unifiers = unifiers;
    }

    @Override
    public Request request() {
        return request;
    }

}

class Path {
    List<Actor<AtomicActor>> completePath;
    int current = 0;

    public Path(List<Actor<AtomicActor>> initialDownstream) {
        completePath = new ArrayList<>(initialDownstream);
    }

    boolean atRoot() {
        return current == 0;
    }

    boolean atEnd() {
        return current == completePath.size() - 1;
    }

    public Actor<AtomicActor> directUpstream() {
        return completePath.get(current - 1);
    }

    public Actor<AtomicActor> directDownstream() {
        if (current == completePath.size() - 1) return null;
        return completePath.get(current + 1);
    }

    void addDownstream(Actor<AtomicActor> downstream) {
        completePath.add(downstream);
    }

    Path moveDownstream() {
        Path path = new Path(completePath);
        path.current = this.current + 1;
        assert path.current < completePath.size() : "Moved downstream past end of path";
        return path;
    }

    Path moveUpstream() {
        Path path = new Path(completePath);
        path.current = this.current - 1;
        assert path.current >= 0 : "Moved upstream past beginning of path";
        return path;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final Path path = (Path) o;
        return current == path.current && Objects.equals(completePath, path.completePath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(completePath, current);
    }
}
