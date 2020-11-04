package grakn.common.poc.reasoning;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

class ResponseProducer {
    private final List<Request> downstreamsAvailable;
    List<Iterator<Long>> traversalProducers;
    private final List<Long> answers;
    int requestsFromUpstream = 0;
    int requestsToDownstream = 0;

    public ResponseProducer() {
        this.downstreamsAvailable = new ArrayList<>();
        this.traversalProducers = new ArrayList<>();
        this.traversalProducers = new ArrayList<>();
        this.answers = new LinkedList<>();
    }

    public void addTraversalProducer(final Iterator<Long> traversalProducer) {
        traversalProducers.add(traversalProducer);
    }

    public void removeTraversalProducer(final Iterator<Long> traversalProducer) {
        traversalProducers.remove(traversalProducer);
    }

    @Nullable
    public Iterator<Long> getOneTraversalProducer() {
        if (!traversalProducers.isEmpty()) return traversalProducers.get(0);
        return null;
    }

    public boolean noMoreAnswersPossible() {
        boolean finished = downstreamsAvailable.isEmpty() && traversalProducers.isEmpty();
        if (finished) assert answers.isEmpty() : "Downstream and traversalProducers are finished, answers should already be sent";
        return finished;
    }

    public void addAvailableDownstream(final Request toDownstream) {
        downstreamsAvailable.add(toDownstream);
    }

    public Request getAvailableDownstream() {
        return downstreamsAvailable.get(0);
    }

    public boolean downstreamDone() {
        return downstreamsAvailable.isEmpty();
    }

    public void downstreamDone(final Request request) {
        downstreamsAvailable.remove(request);
    }

    public void bufferedAnswersAdd(List<Long> answers) {
        this.answers.addAll(answers);
    }

    public int bufferedAnswersSize() {
        return answers.size();
    }

    public Long bufferedAnswersTake() {
        return answers.remove(0);
    }
}
