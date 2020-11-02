package grakn.common.poc.reasoning;


import grakn.common.concurrent.actor.Actor;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class Plan {
    List<Actor<? extends ReasoningActor<?>>> plan;
    int current = 0;

    public Plan(List<Actor<? extends ReasoningActor<?>>> plan) {
        this.plan = new ArrayList<>(plan);
    }

    boolean atStart() {
        return current == 0;
    }

    boolean atEnd() {
        return current == plan.size() - 1;
    }

    Plan toNextStep() {
        Plan plan = new Plan(this.plan);
        plan.current = this.current + 1;
        assert plan.current < plan.plan.size() : "Trying to move past the end of the plan";
        return plan;
    }

    Plan endStepCompleted() {
        assert current == plan.size() - 1 : "Can only complete the end step if it is the last step of the plan";

        Plan plan = new Plan(this.plan);
        plan.current = this.current - 1;
        assert plan.current >= 0 : "Trying to move to before the beginning of the plan";
        return plan;
    }

    public Plan addStep(final Actor<ConjunctiveActor> whenActor) {
        assert current == plan.size() - 1 : "Can only add a step if at the end of the plan";

        Plan plan = new Plan(this.plan);
        plan.plan.add(whenActor);
        return plan;
    }

    public Actor<? extends ReasoningActor<?>> previousStep() {
        return plan.get(current - 1);
    }

    @Nullable
    public Actor<? extends ReasoningActor<?>> nextStep() {
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
