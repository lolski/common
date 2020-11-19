package grakn.common.poc.reasoning.framework;

import grakn.common.collection.Either;
import grakn.common.concurrent.actor.Actor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;

import static grakn.common.collection.Collections.list;
import static grakn.common.collection.Collections.map;

public abstract class Resolver<T extends Resolver<T>> extends Actor.State<T> {
    private static final Logger LOG = LoggerFactory.getLogger(Resolver.class);

    protected String name;
    private boolean isInitialised;
    @Nullable
    private final LinkedBlockingQueue<Response> responses;
    private final Map<Request, ResponseProducer> responseProducers;
    private final Map<Request, Request> requestRouter;

    public Resolver(Actor<T> self, String name) {
        this(self, name, null);
    }

    public Resolver(Actor<T> self, String name, @Nullable LinkedBlockingQueue<Response> responses) {
        super(self);
        this.name = name;
        isInitialised = false;
        this.responses = responses;
        responseProducers = new HashMap<>();
        requestRouter = new HashMap<>();
    }

    public String name() { return name; }

    protected abstract ResponseProducer createResponseProducer(Request fromUpstream);

    protected abstract void initialiseDownstreamActors(Registry registry);

    protected abstract Either<Request, Response> receiveRequest(Request fromUpstream, ResponseProducer responseProducer);

    protected abstract Either<Request, Response> receiveAnswer(Request fromUpstream, Response.Answer fromDownstream, ResponseProducer responseProducer);

    protected abstract Either<Request, Response> receiveExhausted(Request fromUpstream, Response.Exhausted fromDownstream, ResponseProducer responseProducer);

    /*
     *
     * Handlers for messages sent into the execution actor that are dispatched via the actor model.
     *
     */
    public void executeReceiveRequest(Request fromUpstream, Registry registry) {
        LOG.debug("{}: Receiving a new Request from upstream: {}", name, fromUpstream);
        if (!isInitialised) {
            LOG.debug(name + ": initialising downstream actors");
            initialiseDownstreamActors(registry);
            isInitialised = true;
        }

        ResponseProducer responseProducer = responseProducers.computeIfAbsent(fromUpstream, key -> {
            LOG.debug("{}: Creating a new ResponseProducer for the given Request: {}", name, fromUpstream);
            return createResponseProducer(fromUpstream);
        });
        Either<Request, Response> action = receiveRequest(fromUpstream, responseProducer);
        if (action.isFirst()) requestFromDownstream(action.first(), fromUpstream, registry);
        else respondToUpstream(action.second(), registry);
    }

    void executeReceiveAnswer(Response.Answer fromDownstream, Registry registry) {
        LOG.debug("{}: Receiving a new Answer from downstream: {}", name, fromDownstream);
        Request sentDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = requestRouter.get(sentDownstream);
        ResponseProducer responseProducer = responseProducers.get(fromUpstream);
        Either<Request, Response> action = receiveAnswer(fromUpstream, fromDownstream, responseProducer);

        if (action.isFirst()) requestFromDownstream(action.first(), fromUpstream, registry);
        else respondToUpstream(action.second(), registry);
    }

    void executeReceiveExhausted(Response.Exhausted fromDownstream, Registry registry) {
        LOG.debug("{}: Receiving a new Exhausted from downstream: {}", name, fromDownstream);
        Request sentDownstream = fromDownstream.sourceRequest();
        Request fromUpstream = requestRouter.get(sentDownstream);
        ResponseProducer responseProducer = responseProducers.get(fromUpstream);

        Either<Request, Response> action = receiveExhausted(fromUpstream, fromDownstream, responseProducer);

        if (action.isFirst()) requestFromDownstream(action.first(), fromUpstream, registry);
        else respondToUpstream(action.second(), registry);

    }

    /*
     *
     * Helper method private to this class.
     *
     * */
    private void requestFromDownstream(Request request, Request fromUpstream, Registry registry) {
        LOG.debug("{} : Sending a new Request in order to request for an answer from downstream: {}", name, request);
        // TODO we may overwrite if multiple identical requests are sent, when to clean up?
        requestRouter.put(request, fromUpstream);
        Actor<? extends Resolver<?>> receiver = request.receiver();
        receiver.tell(actor -> actor.executeReceiveRequest(request, registry));
    }

    private void respondToUpstream(Response response, Registry registry) {
        LOG.debug("{}: Sending a new Response to respond with an answer to upstream: {}", name, response);
        Actor<? extends Resolver<?>> receiver = response.sourceRequest().sender();
        if (receiver == null) {
            assert responses != null : this + ": can't return answers because the user answers queue is null";
            LOG.debug("{}: Writing a new Response to output queue", name);
            responses.add(response);
        } else {
            if (response.isAnswer()) {
                LOG.debug("{} : Sending a new Response.Answer to upstream", name);
                receiver.tell(actor -> actor.executeReceiveAnswer(response.asAnswer(), registry));
            } else if (response.isExhausted()) {
                LOG.debug("{}: Sending a new Response.Exhausted to upstream", name);
                receiver.tell(actor -> actor.executeReceiveExhausted(response.asExhausted(), registry));
            } else {
                throw new RuntimeException(("Unknown message type " + response.getClass().getSimpleName()));
            }
        }
    }

    public static class Request {
        private final Path path;
        private final List<Long> partialConceptMap;
        private final List<Object> unifiers;
        private final Response.Answer.Resolution partialResolution;

        public Request(Path path,
                       List<Long> partialConceptMap,
                       List<Object> unifiers,
                       Response.Answer.Resolution partialResolution) {
            this.path = path;
            this.partialConceptMap = partialConceptMap;
            this.unifiers = unifiers;
            this.partialResolution = partialResolution;
        }

        public Path path() {
            return path;
        }

        @Nullable
        public Actor<? extends Resolver<?>> sender() {
            if (path.path.size() < 2) {
                return null;
            }
            return path.path.get(path.path.size() - 2);
        }

        public Actor<? extends Resolver<?>> receiver() {
            return path.path.get(path.path.size() - 1);
        }

        public List<Long> partialConceptMap() {
            return partialConceptMap;
        }

        public List<Object> unifiers() {
            return unifiers;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(path, request.path) &&
                    Objects.equals(partialConceptMap, request.partialConceptMap()) &&
                    Objects.equals(unifiers, request.unifiers());
        }

        @Override
        public int hashCode() {
            return Objects.hash(path, partialConceptMap, unifiers);
        }

        @Override
        public String toString() {
            return "Req(send=" + (sender() == null ? "<none>" : sender().state.name) + ", pAns=" + partialConceptMap + ")";
        }

        public Response.Answer.Resolution partialResolutions() {
            return partialResolution;
        }

        public static class Path {
            final List<Actor<? extends Resolver<?>>> path;

            public Path(Actor<? extends Resolver<?>> sender) {
                this(list(sender));
            }

            private Path(List<Actor<? extends Resolver<?>>> path) {
                assert !path.isEmpty() : "Path cannot be empty";
                this.path = path;
            }

            public Path append(Actor<? extends Resolver<?>> actor) {
                List<Actor<? extends Resolver<?>>> appended = new ArrayList<>(path);
                appended.add(actor);
                return new Path(appended);
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                Path path1 = (Path) o;
                return Objects.equals(path, path1.path);
            }

            @Override
            public int hashCode() {
                return Objects.hash(path);
            }
        }
    }

    public static interface Response {
        Request sourceRequest();

        boolean isAnswer();
        boolean isExhausted();

        default Answer asAnswer() {
            throw new ClassCastException("Cannot cast " + this.getClass().getSimpleName() + " to " + Answer.class.getSimpleName());
        }

        default Exhausted asExhausted() {
            throw new ClassCastException("Cannot cast " + this.getClass().getSimpleName() + " to " + Exhausted.class.getSimpleName());
        }

        class Answer implements Response {
            private final Request sourceRequest;
            private final List<Long> conceptMap;
            private final List<Object> unifiers;

            private final String patternAnswered;
            private final Resolution resolution;

            public Answer(Request sourceRequest,
                          List<Long> conceptMap,
                          List<Object> unifiers,
                          String patternAnswered,
                          Resolution resolution) {
                this.sourceRequest = sourceRequest;
                this.conceptMap = conceptMap;
                this.unifiers = unifiers;
                this.patternAnswered = patternAnswered;
                this.resolution = resolution;
            }

            @Override
            public Request sourceRequest() {
                return sourceRequest;
            }

            public List<Long> conceptMap() {
                return conceptMap;
            }

            public List<Object> unifiers() {
                return unifiers;
            }

            public Resolution resolutions() {
                return resolution;
            }

            public boolean isInferred() {
                return !resolution.equals(Resolution.EMPTY);
            }

            @Override
            public boolean isAnswer() { return true; }

            @Override
            public boolean isExhausted() { return false; }

            @Override
            public Answer asAnswer() {
                return this;
            }

            @Override
            public String toString() {
                return "\nAnswer{" +
                        "\nsourceRequest=" + sourceRequest +
                        ",\n partialConceptMap=" + conceptMap +
                        ",\n unifiers=" + unifiers +
                        ",\n patternAnswered=" + patternAnswered +
                        ",\n resolutionsg=" + resolution +
                        '}';
            }

            public static class Resolution {
                public static final Resolution EMPTY = new Resolution(map());

                private Map<Actor<? extends Resolver<?>>, Answer> answers;

                public Resolution(Map<Actor<? extends Resolver<?>>, Answer> answers) {
                    this.answers = map(answers);
                }

                public Resolution withAnswer(Actor<? extends Resolver<?>> producer, Answer answer) {
                    Map<Actor<? extends Resolver<?>>, Answer> copiedResolution = new HashMap<>(answers);
                    copiedResolution.put(producer, answer);
                    return new Resolution(copiedResolution);
                }

                public void update(Map<Actor<? extends Resolver<?>>, Answer> newResolutions) {
                    assert answers.keySet().stream().noneMatch(key -> answers.containsKey(key)) : "Cannot overwrite any resolutions during an update";
                    Map<Actor<? extends Resolver<?>>, Answer> copiedResolutinos = new HashMap<>(answers);
                    copiedResolutinos.putAll(newResolutions);
                    this.answers = copiedResolutinos;
                }

                public void replace(Map<Actor<? extends Resolver<?>>, Answer> newResolutions) {
                    this.answers = map(newResolutions);
                }

                public Map<Actor<? extends Resolver<?>>, Answer> answers() {
                    return this.answers;
                }

                @Override
                public String toString() {
                    return "Resolutions{" + "answers=" + answers + '}';
                }
            }
        }

        class Exhausted implements Response {
            private final Request sourceRequest;

            public Exhausted(Request sourceRequest) {
                this.sourceRequest = sourceRequest;
            }

            @Override
            public Request sourceRequest() {
                return sourceRequest;
            }

            @Override
            public boolean isAnswer() { return false; }

            @Override
            public boolean isExhausted() { return true; }

            @Override
            public Exhausted asExhausted() {
                return this;
            }


            @Override
            public String toString() {
                return "Exhausted{" +
                        "sourceRequest=" + sourceRequest +
                        '}';
            }
        }
    }
}
