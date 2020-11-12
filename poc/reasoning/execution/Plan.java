package grakn.common.poc.reasoning.execution;


import grakn.common.concurrent.actor.Actor;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

// TODO make this state immutable
public class Plan {
    private static final int NOT_STARTED = -1;

    List<Actor<? extends ExecutionActor<?>>> plan;
    int current = NOT_STARTED;

    public Plan(List<Actor<? extends ExecutionActor<?>>> plan) {
        this.plan = new ArrayList<>(plan);
    }

    public boolean atEnd() {
        return current == plan.size() - 1;
    }

    public boolean isEmpty() {
        boolean isEmpty = plan.size() == 0;
        if (isEmpty) {
            assert current == NOT_STARTED : "An empty plan's current position must be -1";
        }
        return isEmpty;
    }

    public Plan toNextStep() {
        Plan plan = new Plan(this.plan);
        plan.current = this.current + 1;
        assert plan.current < plan.plan.size() : "Trying to move past the last step of the plan";
        return plan;
    }

    public Plan endStepCompleted() {
        assert current != NOT_STARTED;
        assert current == plan.size() - 1 : "Can only complete the end step if it is the last step of the plan";

        Plan plan = new Plan(this.plan.subList(0, this.plan.size() - 1));
        plan.current = this.current - 1;
        assert plan.current >= NOT_STARTED : "Trying to move to the position before the first step of the plan";
        return plan;
    }

    public Plan truncate() {
        assert current != NOT_STARTED;

        Plan plan = new Plan(this.plan.subList(0, current + 1));
        plan.current = this.current;
        return plan;
    }

    public Plan addStep(final Actor<? extends ExecutionActor<?>> actor) {
        assert current == plan.size() - 1 : "Can only add a step if at the last step of the plan";
        return addSteps(Arrays.asList(actor));
    }

    public Plan addSteps(final List<? extends Actor<? extends ExecutionActor<?>>> actors) {
        assert current == plan.size() - 1 : "Can only add a step if at the last step of the plan";

        Plan plan = new Plan(this.plan);
        plan.plan.addAll(actors);
        plan.current = this.current;
        return plan;
    }

    @Nullable
    public Actor<? extends ExecutionActor<?>> previousStep() {
        if (current == 0) return null;
        return plan.get(current - 1);
    }

    @Nullable
    public Actor<? extends ExecutionActor<?>> currentStep() {
        if (current == NOT_STARTED) return null;
        return plan.get(current);
    }

    @Nullable
    public Actor<? extends ExecutionActor<?>> nextStep() {
        if (current == plan.size() - 1) return null;
        return plan.get(current + 1);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final Plan plan = (Plan) o;
        return current == plan.current && Objects.equals(this.plan, plan.plan);
    }

    @Override
    public int hashCode() {
        return Objects.hash(plan, current);
    }
}
