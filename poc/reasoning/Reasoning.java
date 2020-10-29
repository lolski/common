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

    private Map<NextMessage, Todos> todos;
    private Long queryPattern;

    public AtomicActor(final Actor<AtomicActor> self, Long queryPattern) {
        super(self);
        this.queryPattern = queryPattern;
        // the query pattern long represents some thing to pass to a traversal or resolve further
        todos = new HashMap<>();
    }

    public void next(NextMessage request) {
        if (!todos.containsKey(request)) {

            // initialise traversal
            Iterator<Long> traversal = (new MockTransaction(10, 1)).query();
            List<ResumeLocalProducer> producers = Arrays.asList(new ResumeTraversal(request));

            // initialise rules
            for (Actor<RuleActor> rule : getApplicableRuleActors(request)) {
                producers.add(new ResumeRule(request, rule));
            }

            // record the new todo
            Todos todo = new Todos(traversal, producers);
            todos.put(request, todo);
        }

        dispatchProducers(request);
    }

    private void dispatchProducers(NextMessage request) {
        Todos todo = todos.get(request);
        int availableProducers = todo.localProducers.size();
        // TODO note that this may not dispatch enough sub-requests if there are fewer processors than there are requests
        // however, we will dispatch more producers as required when we receive a DONE or an ANSWER as well?
        for (int i = 0; i < availableProducers; i++) {
            // TODO if we want to do pre-compute, we can request more than the requested number answers at once
            if (todo.requested > todo.dispatchedProducers) {
                // TODO choose in what order to activate the available active producers
                // for now, do round robin

                ResumeLocalProducer resumption = todo.localProducers.get(i);

                if (resumption instanceof ResumeRule) {
                    self().tell((actor -> actor.resumeRule((ResumeRule) resumption)));
                } else if (resumption instanceof ResumeTraversal) {
                    self().tell(actor -> actor.resumeTraversal((ResumeTraversal) resumption));
                } else {
                    throw new RuntimeException("Unknown resume: " + resumption);
                }

                // at this point, we've dispatched messages to produce new answers
                // record the number of in-processing
                todo.dispatchedProducers++;
            }
        }
    }


    private List<Actor<RuleActor>> getApplicableRuleActors(final NextMessage request) {
        // TODO find some applicable rules
        return Arrays.asList();
    }

    public void done(DoneMessage done) {

    }

    public void answer(AnswerMessage answer) {

    }

    public void resumeTraversal(ResumeTraversal resumeTraversal) {
        NextMessage source = resumeTraversal.source;
        Todos todo = this.todos.get(source);
        if (todo.traversal.hasNext()) {
            Long answer = todo.traversal.next();
            answer += this.queryPattern;
            // TODO send answer if requested, else buffer
            // TODO decrement processing
        } else {
            todo.traversalCompleted(resumeTraversal);
        }

        if (todo.finished()) {
            Actor<AtomicActor> requester = source.returnPath.get(source.returnPath.size() - 1);
            DoneMessage doneMessage = new DoneMessage(source);
            requester.tell((actor) -> actor.done(doneMessage));
        }
    }

    public void resumeRule(ResumeRule resumeRule) {
        NextMessage source = resumeRule.source;
        Todos todo = this.todos.get(source);
        if (!todo.isRuleCompleted(resumeRule)) {
            Actor<RuleActor> ruleActor = resumeRule.ruleActor;
            // request the next answer from the rule actor
            List<Actor<AtomicActor>> returnPath = new ArrayList<>(source.returnPath.size() + 1);
            returnPath.addAll(source.returnPath);
            returnPath.add(self());

            NextMessage request = new NextMessage(
                    returnPath,
                    source.gotoPath,
                    source.partialAnswers,
                    source.constraints,
                    source.unifiers
            );

            ruleActor.tell((actor) -> actor.next(request));
        }
    }
}

class RuleActor extends Actor.State<RuleActor> {

    // Rule TODOs can be simpler
    private Map<NextMessage, Todos> todos;

    protected RuleActor(final Actor<RuleActor> self) {
        super(self);
    }

    public void next(NextMessage request) {

    }

    public void answer(AnswerMessage answer) {

    }

    public void done(DoneMessage done) {

    }
}


class Todos {
    Iterator<Long> traversal;
    List<ResumeLocalProducer> localProducers; // only shrinks when a DONE or traversal hasNext() == false
    List<Long> answers = new LinkedList<>();
    int requested = 0;
    int dispatchedProducers = 0;

    public Todos(Iterator<Long> traversal, List<ResumeLocalProducer> localProducers) {
        this.traversal = traversal;
        this.localProducers = localProducers;
    }

    public boolean isRuleCompleted(ResumeRule rule) {
        return localProducers.contains(rule);
    }

    public void ruleCompleted(ResumeRule rule) {
        localProducers.remove(rule);
    }

    public void traversalCompleted(ResumeTraversal resume) {
        localProducers.remove(resume);
    }

    public boolean finished() {
        return localProducers.isEmpty();
    }
}


class ResumeLocalProducer { }
class ResumeTraversal extends ResumeLocalProducer {
    NextMessage source;
    public ResumeTraversal(NextMessage source) {
        this.source = source;
    }
}
class ResumeRule extends ResumeLocalProducer {
    NextMessage source;
    Actor<RuleActor> ruleActor;

    public ResumeRule(NextMessage source, Actor<RuleActor> ruleActor) {
        this.source = source;
        this.ruleActor = ruleActor;
    }
}


interface Request { }

interface Response {
    Request request();
}

class NextMessage implements Request {
    final List<Actor<AtomicActor>> returnPath;
    final List<Actor<AtomicActor>> gotoPath;
    final List<Long> partialAnswers;
    final List<Object> constraints;
    final List<Object> unifiers;

    public NextMessage(List<Actor<AtomicActor>> returnPath,
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

class DoneMessage implements Response {
    private NextMessage request;
    public DoneMessage(NextMessage request) {
        this.request = request;
    }

    @Override
    public NextMessage request() {
        return request;
    }
}

class AnswerMessage implements Response {
    private final NextMessage request;
    private final List<Actor<AtomicActor>> returnPath;
    private final List<Actor<AtomicActor>> gotoPath;
    private final List<Long> partialAnswers;
    private final List<Object> constraints;
    private final List<Object> unifiers;

    public AnswerMessage(NextMessage request,
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
    public NextMessage request() {
        return request;
    }
}

