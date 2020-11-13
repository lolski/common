package grakn.common.poc.reasoning.execution;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class ResponseProducer {
    private final Set<Request> readyDownstreamRequests;
    private final Iterator<List<Long>> traversalProducer;
    private Iterator<Request> nextProducer;

    public ResponseProducer(Iterator<List<Long>> traversalProducer) {
        this.readyDownstreamRequests = new HashSet<>();
        this.traversalProducer = traversalProducer;
        nextProducer = readyDownstreamRequests.iterator();
    }

    public boolean hasTraversalProducer() {
        return traversalProducer.hasNext();
    }

    public Iterator<List<Long>> traversalProducer() {
        return traversalProducer;
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
