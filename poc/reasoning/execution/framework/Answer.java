package grakn.common.poc.reasoning.execution.framework;

import grakn.common.concurrent.actor.Actor;

import java.util.List;

public class Answer {
    private final List<Long> conceptMap;
    private final ExecutionRecord executionRecord;
    private final Actor<? extends ExecutionActor<?>> producer;
    private final String patternAnswered;

    public Answer(List<Long> conceptMap,
                  String patternAnswered,
                  ExecutionRecord executionRecord,
                  Actor<? extends ExecutionActor<?>> producer) {
        this.conceptMap = conceptMap;
        this.patternAnswered = patternAnswered;
        this.executionRecord = executionRecord;
        this.producer = producer;
    }

    public List<Long> conceptMap() {
        return conceptMap;
    }

    public ExecutionRecord executionRecord() {
        return executionRecord;
    }

    public boolean isInferred() {
        return !executionRecord.equals(ExecutionRecord.EMPTY);
    }

    public Actor<? extends ExecutionActor<?>> producer() {
        return producer;
    }

    @Override
    public String toString() {
        return "Answer{" +
                "conceptMap=" + conceptMap +
                ", executionRecord=" + executionRecord +
                ", patternAnswered='" + patternAnswered + '\'' +
                ", producer=" + producer +
                '}';
    }
}
