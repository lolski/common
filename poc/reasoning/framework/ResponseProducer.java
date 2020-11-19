package grakn.common.poc.reasoning.framework;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class ResponseProducer {
    private final Set<List<Long>> produced;
    private final Iterator<List<Long>> traversalProducer;
    private final Set<Resolver.Request> downstreamProducer;
    private Iterator<Resolver.Request> downstreamProducerSelector;

    public ResponseProducer(Iterator<List<Long>> traversalProducer) {
        produced = new HashSet<>();
        this.traversalProducer = traversalProducer;
        downstreamProducer = new HashSet<>();
        downstreamProducerSelector = downstreamProducer.iterator();
    }

    public void recordProduced(List<Long> conceptMap) {
        produced.add(conceptMap);
    }

    public boolean hasProduced(List<Long> conceptMap) {
        return produced.contains(conceptMap);
    }

    public boolean hasTraversalProducer() {
        return traversalProducer.hasNext();
    }

    public Iterator<List<Long>> traversalProducer() {
        return traversalProducer;
    }

    public boolean hasDownstreamProducer() {
        return !downstreamProducer.isEmpty();
    }

    public Resolver.Request nextDownstreamProducer() {
        if (!downstreamProducerSelector.hasNext()) downstreamProducerSelector = downstreamProducer.iterator();
        return downstreamProducerSelector.next();
    }

    public void addDownstreamProducer(Resolver.Request request) {
        assert !(downstreamProducer.contains(request)) : "downstream answer producer already contains this request";

        downstreamProducer.add(request);
        downstreamProducerSelector = downstreamProducer.iterator();
    }

    public void removeDownstreamProducer(Resolver.Request request) {
        boolean removed = downstreamProducer.remove(request);
        // only update the iterator when removing an element, to avoid resetting and reusing first request too often
        // note: this is a large performance win when processing large batches of requests
        if (removed) downstreamProducerSelector = downstreamProducer.iterator();
    }
}
