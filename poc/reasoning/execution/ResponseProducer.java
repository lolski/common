package grakn.common.poc.reasoning.execution;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ResponseProducer {
    private final List<Request> readyDownstreamRequests;
    private List<Iterator<List<Long>>> traversalProducers;
    private int nextDownstream;

    public ResponseProducer() {
        this.readyDownstreamRequests = new ArrayList<>();
        this.traversalProducers = new ArrayList<>();
        this.nextDownstream = 0;
    }

    public void addTraversalProducer(final Iterator<List<Long>> traversalProducer) {
        traversalProducers.add(traversalProducer);
    }

    public void removeTraversalProducer(final Iterator<List<Long>> traversalProducer) {
        traversalProducers.remove(traversalProducer);
        this.nextDownstream = 0;
    }

    @Nullable
    public Iterator<List<Long>> getOneTraversalProducer() {
        if (!traversalProducers.isEmpty()) return traversalProducers.get(0);
        return null;
    }

    public void addReadyDownstream(final Request toDownstream) {
        readyDownstreamRequests.add(toDownstream);
    }

    public Request getReadyDownstreamRequest() {
        Request downstreamRequest = readyDownstreamRequests.get(nextDownstream);
        if (readyDownstreamRequests.size() > 1) nextDownstream = (nextDownstream + 1) % (readyDownstreamRequests.size() - 1);
        return downstreamRequest;
    }

    public boolean hasReadyDownstreamRequest() {
        return !readyDownstreamRequests.isEmpty();
    }

    public void removeReadyDownstream(final Request request) {
        readyDownstreamRequests.remove(request);
    }
}
