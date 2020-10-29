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

            // initialise traversal
            Iterator<Long> traversal = (new MockTransaction(10, 1)).query();
            List<ResumeLocalProducer> producers = Arrays.asList(new ResumeTraversal(request));

            // initialise rules
            for (Actor<RuleActor> rule : getApplicableRuleActors(request)) {
                producers.add(new ResumeRule(request, rule));
            }

            // record the new responseProducer
            ResponseProducer responseProducer = new ResponseProducer(traversal, producers);
            responseProducers.put(request, responseProducer);
        }

        dispatchProducers(request);
    }

    public void responseDone(ResponseDone done) {
        Request request = done.request();
        ResponseProducer responseProducer = responseProducers.get(request);

        if (responseProducer.finished()) {
            dispatchDone(request);
        }
    }

    public void responseAnswer(ResponseAnswer answer) {

    }

    private void resumeTraversal(ResumeTraversal resumeTraversal) {
        Request source = resumeTraversal.source;
        ResponseProducer responseProducer = this.responseProducers.get(source);
        if (responseProducer.localTraversal.hasNext()) {
            Long answer = responseProducer.localTraversal.next();
            answer += this.queryPattern;
            // TODO send answer if requested, else buffer
            // TODO decrement processing
        } else {
            responseProducer.traversalDone(resumeTraversal);
        }

        if (responseProducer.finished()) {
            dispatchDone(source);
        }
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

    private void dispatchProducers(Request request) {
        ResponseProducer responseProducer = responseProducers.get(request);
        int availableProducers = responseProducer.producers.size();
        // TODO note that this may not dispatch enough sub-requests if there are fewer processors than there are requests
        // however, we will dispatch more producers as required when we receive a DONE or an ANSWER as well?
        for (int i = 0; i < availableProducers; i++) {
            // TODO if we want to do pre-compute, we can request more than the requested number answers at once
            if (responseProducer.requested > responseProducer.dispatched) {
                // TODO choose in what order to activate the available active producers
                // for now, do round robin

                ResumeLocalProducer resumption = responseProducer.producers.get(i);

                if (resumption instanceof ResumeRule) {
                    self().tell((actor -> actor.resumeRule((ResumeRule) resumption)));
                } else if (resumption instanceof ResumeTraversal) {
                    self().tell(actor -> actor.resumeTraversal((ResumeTraversal) resumption));
                } else {
                    throw new RuntimeException("Unknown resume: " + resumption);
                }

                // at this point, we've dispatched messages to produce new answers
                // record the number of in-processing
                responseProducer.dispatched++;
            }
        }
    }

    private List<Actor<RuleActor>> getApplicableRuleActors(final Request request) {
        // TODO find some applicable rules
        return Arrays.asList();
    }

    private void dispatchDone(final Request request) {
        Actor<AtomicActor> requester = request.returnPath.get(request.returnPath.size() - 1);
        ResponseDone responseDone = new ResponseDone(request);
        requester.tell((actor) -> actor.responseDone(responseDone));
    }
}

class RuleActor extends Actor.State<RuleActor> {

    // Rule TODOs can be simpler
    private Map<Request, ResponseProducer> responseProducers;

    protected RuleActor(final Actor<RuleActor> self) {
        super(self);
    }

    public void next(Request request) {

    }

    public void answer(ResponseAnswer answer) {

    }

    public void done(ResponseDone done) {

    }
}


class ResponseProducer {
    Iterator<Long> localTraversal;
    List<ResumeLocalProducer> producers; // only shrinks when a DONE or traversal hasNext() == false
    List<Long> answers = new LinkedList<>();
    int requested = 0;
    int dispatched = 0;

    public ResponseProducer(Iterator<Long> localTraversal, List<ResumeLocalProducer> producers) {
        this.localTraversal = localTraversal;
        this.producers = producers;
    }

    public boolean isRuleProducerDone(ResumeRule rule) {
        return producers.contains(rule);
    }

    public void ruleProducerDone(ResumeRule rule) {
        producers.remove(rule);
    }

    public void traversalDone(ResumeTraversal resume) {
        producers.remove(resume);
    }

    public boolean finished() {
        return producers.isEmpty();
    }
}


class ResumeLocalProducer { }
class ResumeTraversal extends ResumeLocalProducer {
    Request source;
    public ResumeTraversal(Request source) {
        this.source = source;
    }
}
class ResumeRule extends ResumeLocalProducer {
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

