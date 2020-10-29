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

    private Map<Request, ResponseProducer> responseProducers;
    private Long queryPattern;

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

        if (responseProducer.requested > responseProducer.dispatched + responseProducer.answers.size()) {
            dispatchRuleRequests(request, responseProducer);
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

    private void dispatchRuleRequests(final Request request, final ResponseProducer responseProducer) {
        for (Actor<RuleActor> ruleActor : responseProducer.ruleProducers) {
            // request the next answer from the rule actor
            List<Actor<AtomicActor>> returnPath = new ArrayList<>(request.returnPath.size() + 1);
            returnPath.addAll(request.returnPath);
            returnPath.add(self());

            Request nextAnswerRequest = new Request(
                    returnPath,
                    request.gotoPath,
                    request.partialAnswers,
                    request.constraints,
                    request.unifiers,
                    ruleActor
            );

            ruleActor.tell((actor) -> actor.receiveRequest(nextAnswerRequest));
            responseProducer.dispatched++;
        }
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

    private List<Actor<RuleActor>> getApplicableRuleActors(final Request request) {
        // TODO find some applicable rules
        return Arrays.asList();
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
        List<Actor<RuleActor>> ruleProducers = getApplicableRuleActors(request);
        return new ResponseProducer(traversal, ruleProducers);
    }
}

class RuleActor extends Actor.State<RuleActor> {

    // Rule TODOs can be simpler
    private Map<Request, ResponseProducer> responseProducers;

    protected RuleActor(final Actor<RuleActor> self) {
        super(self);
    }

    public void receiveRequest(Request request) {

    }

    public void receiveAnswer(ResponseAnswer answer) {

    }

    public void receiveDone(ResponseDone done) {

    }
}


class ResponseProducer {
    Iterator<Long> traversalProducer;
    List<Actor<RuleActor>> ruleProducers;
    List<Long> answers = new LinkedList<>();
    int requested = 0;
    int dispatched = 0;

    public ResponseProducer(Iterator<Long> traversalProducer, List<Actor<RuleActor>> ruleProducers) {
        this.traversalProducer = traversalProducer;
        this.ruleProducers = ruleProducers;
    }

    public boolean isRuleProducerDone(ResumeRule rule) {
        return ruleProducers.contains(rule);
    }

    public void ruleProducerDone(ResumeRule rule) {
        ruleProducers.remove(rule);
    }

    public void traversalDone(ResumeTraversal resume) {
        ruleProducers.remove(resume);
    }

    public boolean finished() {
        return ruleProducers.isEmpty() && !traversalProducer.hasNext();
    }
}


class ResumeRule {
    Request source;
    Actor<RuleActor> ruleActor;

    public ResumeRule(Request source, Actor<RuleActor> ruleActor) {
        this.source = source;
        this.ruleActor = ruleActor;
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
    Actor<RuleActor> requester;

    public Request(List<Actor<AtomicActor>> returnPath,
                   List<Actor<AtomicActor>> gotoPath,
                   List<Long> partialAnswers,
                   List<Object> constraints,
                   List<Object> unifiers,
                   Actor<RuleActor> localRequester) {

        this.returnPath = returnPath;
        this.gotoPath = gotoPath;
        this.partialAnswers = partialAnswers;
        this.constraints = constraints;
        this.unifiers = unifiers;
        this.requester = localRequester;
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

