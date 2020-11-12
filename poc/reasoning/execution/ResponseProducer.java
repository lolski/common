package grakn.common.poc.reasoning.execution;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ResponseProducer {
    private final List<Request> readyDownstreamRequests;
    private List<Iterator<Long>> traversalProducers;

    public ResponseProducer() {
        this.readyDownstreamRequests = new ArrayList<>();
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

    public void addReadyDownstream(final Request toDownstream) {
        readyDownstreamRequests.add(toDownstream);
    }

    public Request getReadyDownstreamRequest() {
        return readyDownstreamRequests.get(0);
    }

    public boolean hasReadyDownstreamRequest() {
        return !readyDownstreamRequests.isEmpty();
    }

    public void removeReadyDownstream(final Request request) {
        readyDownstreamRequests.remove(request);
    }
}
