package grakn.common.poc.reasoning;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

class ResponseProducer {
    List<Iterator<Long>> traversalProducers;
    boolean downstreamDone;
    List<Long> answers = new LinkedList<>();
    int requestsFromUpstream = 0;
    int requestsToDownstream = 0;

    public ResponseProducer(boolean hasDownstream) {
        this.traversalProducers = new ArrayList<>();
        this.downstreamDone = !hasDownstream;
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
        return downstreamDone == true && traversalProducers.isEmpty();
    }

    public void setDownstreamDone() {
        downstreamDone = true;
    }

    public boolean isDownstreamDone() {
        return downstreamDone;
    }

}
