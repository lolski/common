package grakn.common.poc.reasoning;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

class ResponseProducer {
    private final List<Request> downstreamsAvailable;
    private List<Iterator<Long>> traversalProducers;
    private int requestsFromUpstream = 0;
    private int requestsToDownstream = 0;

    public ResponseProducer() {
        this.downstreamsAvailable = new ArrayList<>();
        this.traversalProducers = new ArrayList<>();
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
        return downstreamsAvailable.isEmpty() && traversalProducers.isEmpty();
    }

    public void addAvailableDownstream(final Request toDownstream) {
        downstreamsAvailable.add(toDownstream);
    }

    public Request getAvailableDownstream() {
        return downstreamsAvailable.get(0);
    }

    public boolean downstreamsExhausted() {
        return downstreamsAvailable.isEmpty();
    }

    public void downstreamExhausted(final Request request) {
        downstreamsAvailable.remove(request);
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
