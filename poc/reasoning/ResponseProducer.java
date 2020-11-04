package grakn.common.poc.reasoning;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

class ResponseProducer {
    private final List<Request> downstreamsAvailable;
    private List<Iterator<Long>> traversalProducers;
    private final List<Long> bufferedAnswers;
    private int requestsFromUpstream = 0;
    private int requestsToDownstream = 0;

    public ResponseProducer() {
        this.downstreamsAvailable = new ArrayList<>();
        this.traversalProducers = new ArrayList<>();
        this.traversalProducers = new ArrayList<>();
        this.bufferedAnswers = new LinkedList<>();
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
        boolean isEmpty = downstreamsAvailable.isEmpty() && traversalProducers.isEmpty();
        if (isEmpty) assert bufferedAnswers.isEmpty() : "Downstream and traversalProducers are finished, answers should already be sent";
        return isEmpty;
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

    public void bufferAnswers(List<Long> answers) {
        this.bufferedAnswers.addAll(answers);
    }

    public void bufferAnswer(Long answer) {
        this.bufferedAnswers.add(answer);
    }

    public int bufferedSize() {
        return bufferedAnswers.size();
    }

    public Long bufferTake() {
        return bufferedAnswers.remove(0);
    }

    public int requestsFromUpstream() {
        return requestsFromUpstream;
    }

    public void incrementRequestsFromUpstream() {
        requestsFromUpstream++;
    }

    public void decrementRequestsFromUpstream() {
        requestsFromUpstream--;
    }

    public int requestsToDownstream() {
        return requestsToDownstream;
    }

    public void incrementRequestsToDownstream() {
        requestsToDownstream++;
    }

    public void decrementRequestsToDownstream() {
        requestsToDownstream--;
    }
}
