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

    public void request(Request request) {
        if (!responseProducers.containsKey(request)) {
            responseProducers.put(request, createResponseProducer(request));
        }
        ResponseProducer responseProducer = responseProducers.get(request);

        // TODO if we want batching, we increment by as many as are requested
        responseProducer.requested++;

        process(request);
    }

    public void responseDone(ResponseDone done) {
        Request request = done.request();
        ResponseProducer responseProducer = responseProducers.get(request);

        if (responseProducer.finished()) {
            dispatchResponseDone(request);
        }
    }

    public void responseAnswer(ResponseAnswer answer) {

    }

    private void resumeTraversal(ResumeTraversal resumeTraversal) {
        Request source = resumeTraversal.source;
        ResponseProducer responseProducer = this.responseProducers.get(source);

    }

    private void resumeRule(ResumeRule resumeRule) {
        Request source = resumeRule.source;
        ResponseProducer responseProducer = this.responseProducers.get(source);
        if (!responseProducer.isRuleProducerDone(resumeRule)) {
            Actor<RuleActor> ruleActor = resumeRule.ruleActor;
            // request the next answer from the rule actor
            List<Actor<AtomicActor>> returnPath = new ArrayList<>(source.returnPath.size() + 1);
            returnPath.addAll(source.returnPath);
            returnPath.add(self());

            Request request = new Request(
                    returnPath,
                    source.gotoPath,
                    source.partialAnswers,
                    source.constraints,
                    source.unifiers,
                    resumeRule
            );

            ruleActor.tell((actor) -> actor.next(request));
        }

    }

    private void process(Request request) {
        ResponseProducer responseProducer = responseProducers.get(request);
        dispatchResponseAnswers(request, responseProducer);

        if (responseProducer.requested > responseProducer.dispatched + responseProducer.answers.size()) {
            produceTraversalAnswers(responseProducer);
        }

        if (responseProducer.requested > responseProducer.dispatched + responseProducer.answers.size()) {
            dispatchRuleRequests(responseProducer);
        }

        dispatchResponseAnswers(request, responseProducer);

        if (responseProducer.finished()) {
            dispatchResponseDone(request);
        }
    }

    private void dispatchRuleRequests(final ResponseProducer responseProducer) {
        for (Actor<RuleActor> ruleActor : responseProducer.ruleProducers) {
            // TODO fill in REQUEST
            Request nextAnswerRequest = null;
            ruleActor.tell((actor) -> actor.request(nextAnswerRequest));
            responseProducer.dispatched++;
        }
    }

    private void produceTraversalAnswers(final ResponseProducer responseProducer) {
        // choose some strategy to answer the request
        if (responseProducer.traversalProducer.hasNext()) {
            Long answer = responseProducer.traversalProducer.next();
            answer += this.queryPattern;
            responseProducer.answers.add(answer);
        }
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

            requester.tell((actor) -> actor.responseAnswer(responseAnswer));
        }
    }

    private List<Actor<RuleActor>> getApplicableRuleActors(final Request request) {
        // TODO find some applicable rules
        return Arrays.asList();
    }

    private void dispatchResponseDone(final Request request) {
        Actor<AtomicActor> requester = request.returnPath.get(request.returnPath.size() - 1);
        ResponseDone responseDone = new ResponseDone(request);
        requester.tell((actor) -> actor.responseDone(responseDone));
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

    public void request(Request request) {

    }

    public void answer(ResponseAnswer answer) {

    }

    public void done(ResponseDone done) {

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
    ResumeLocalProducer localRequester;

    public Request(List<Actor<AtomicActor>> returnPath,
                   List<Actor<AtomicActor>> gotoPath,
                   List<Long> partialAnswers,
                   List<Object> constraints,
                   List<Object> unifiers,
                   ResumeLocalProducer localRequester) {

        this.returnPath = returnPath;
        this.gotoPath = gotoPath;
        this.partialAnswers = partialAnswers;
        this.constraints = constraints;
        this.unifiers = unifiers;
        this.localRequester = localRequester;
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

