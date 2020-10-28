package grakn.common.poc.reasoning;

import grakn.common.concurrent.actor.Actor;

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
            todo.resumeDone(resumeTraversal);
        }

        if (todo.finished()) {
            // TODO send DONE
        }
    }

    public void resumeRule(ResumeRule resumeRule) {

    }
}


class Todos {
    Iterator<Long> traversal;
    List<Resume> resumeCompute; // only modified when a DONE or traversal hasNext() == false lets us remove a Continue
    List<Long> answers = new LinkedList<>();
    int requested = 0;
    int processing = 0;

    public Todos(Iterator<Long> traversal, List<Resume> initialResume) {
        this.traversal = traversal;
        this.resumeCompute = initialResume;
    }

    public void ruleDone(ResumeRule rule) {
        resumeCompute.remove(rule);
    }

    public void resumeDone(ResumeTraversal resume) {
        resumeCompute.remove(resume);
    }

    public boolean finished() {
        return resumeCompute.isEmpty();
    }
}


class Resume { }
class ResumeTraversal extends Resume {
    NextMessage source;
    public ResumeTraversal(NextMessage source) {
        this.source = source;
    }
}
class ResumeRule extends Resume {
    NextMessage source;
    private Actor<?> ruleActor;

    public ResumeRule(NextMessage source, Actor<?> ruleActor) {
        this.source = source;
        this.ruleActor = ruleActor;
    }
}

interface Request { }

interface Response {
    Request request();
}

class NextMessage implements Request {
    private final List<Actor<AtomicActor>> returnPath;
    private final List<Actor<AtomicActor>> gotoPath;
    private final List<Long> partialAnswers;
    private final List<Object> constraints;
    private final List<Object> unifiers;

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

