package grakn.common.poc.reasoning.execution;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class ResponseProducer {
    private final Set<Request> readyDownstreamRequests;
    private List<Iterator<List<Long>>> traversalProducers;
    private Iterator<Request> nextProducer;
    public ResponseProducer() {
        this.readyDownstreamRequests = new HashSet<>();
        this.traversalProducers = new ArrayList<>();
        nextProducer = readyDownstreamRequests.iterator();
    }

    public void addTraversalProducer(final Iterator<List<Long>> traversalProducer) {
        traversalProducers.add(traversalProducer);
    }

    public void removeTraversalProducer(final Iterator<List<Long>> traversalProducer) {
        traversalProducers.remove(traversalProducer);
    }

    @Nullable
    public Iterator<List<Long>> getOneTraversalProducer() {
        if (!traversalProducers.isEmpty()) return traversalProducers.get(0);
        return null;
    }

    public void addReadyDownstream(final Request toDownstream) {
        assert !(readyDownstreamRequests.contains(toDownstream)) : "ready downstream requests already contains this request";

        readyDownstreamRequests.add(toDownstream);
        nextProducer = readyDownstreamRequests.iterator();
    }

    public Request getReadyDownstreamRequest() {
        if (!nextProducer.hasNext()) nextProducer = readyDownstreamRequests.iterator();
        return nextProducer.next();
    }

    public boolean hasReadyDownstreamRequest() {
        return !readyDownstreamRequests.isEmpty();
    }

    public void removeReadyDownstream(final Request request) {
        boolean removed = readyDownstreamRequests.remove(request);
        // only update the iterator when removing an element, to avoid resetting and reusing first request too often
        // note: this is a large performance win when processing large batches of requests
        if (removed) nextProducer = readyDownstreamRequests.iterator();
    }
}
