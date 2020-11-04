package grakn.common.poc.reasoning;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

class ResponseProducer {
    private List<Request> downstreamsAvailable;
    List<Iterator<Long>> traversalProducers;
    List<Long> answers;
    int requestsFromUpstream = 0;
    int requestsToDownstream = 0;

    public ResponseProducer() {
        this.downstreamsAvailable = new ArrayList<>();
        this.traversalProducers = new ArrayList<>();
        this.traversalProducers = new ArrayList<>();
        this.answers = new LinkedList<>();
    }

    public void addTraversalProducer(Iterator<Long> traversalProducer) {
        traversalProducers.add(traversalProducer);
    }

    @Nullable
    public Iterator<Long> getOneTraversalProducer() {
        if (!traversalProducers.isEmpty()) return traversalProducers.get(0);
        return null;
    }

    public void removeTraversalProducer(Iterator<Long> traversalProducer) {
        traversalProducers.remove(traversalProducer);
    }

    public boolean finished() {
        boolean finished = downstreamsAvailable.isEmpty() && traversalProducers.isEmpty();
        if (finished) assert answers.isEmpty() : "Downstream and traversalProducers are finished, answers should already be sent";
        return finished;
    }

    public boolean isDownstreamDone() {
        return downstreamsAvailable.isEmpty();
    }

    public void addAvailableDownstream(final Request toDownstream) {
        downstreamsAvailable.add(toDownstream);
    }

    public void downstreamDone(final Request request) {
        downstreamsAvailable.remove(request);
    }

    public Request getAvailableDownstream() {
        return downstreamsAvailable.get(0);
    }
}
