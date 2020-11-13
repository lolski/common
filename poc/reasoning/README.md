
## ADR: Reasoning Actor Communication Models

### Linearised Branching Messaging Model

In this approach, a Conjunction (`A and B`) pre-plans how messages it sends should travel through two sub-queries
`A` and `B`. Messages must follow this order, and the plan must be fully executed, before traversal and rule
processing can occur. Messages must then re-trace their plan backwards to return.

For example:

`C` = `A and B` is the root conjunction. There are two sub-queries `A` and `B`. The conjunctions plans that `A` must be
executed before `B` can executed. 

`C` sends messages containing the plan `[A, B]` to `B`, which will send the message to `A`. `A`, at the end of the plan,
can perform traversal and execute rules. `A` sends answers backwards along the plan, to `B`, which computes its owner,
and sends it in combination with the answer from `A` to `C`. `C` can then respond upstream with the full answer.

Pros:
  * Equally load balances message-passing work among all actors. Given the assumption of 1 request leading to 1 answer,
    every conjunction will receive N requests, and send N requests downstream. `B`, will receive N requests, and send N
    requests downstream. `A` receives N requests, and sends N responses upstream. `B` then receives them and sends N
    answers upstream. Finally, `C` receives those, and sends N upstream.
    On average, every actor receives or sends T/4 messages (T is total number of messages). This equal loading
    means that actors can be well-parallelised
    
Cons:
  * Mental model is quite difficult compared to a non-linearised reasoning model. (This is important to us)
  * Related to prior point, explanations become non-trivial to implement and are most easily produced in a nested structure,
    which does not align with our desired API and query structure.


### Natural Tree Messaging Model

Example (compare to above):
`C` = `A and B`. It plans `A` and `B`, and sends requests to `A` first. The message contains no plan, but only a path that
encodes the sender and receiver. `A` immediately sends answers back to `C`. `C` checks the sender
and forwards the answer in the form a new message to `B`. `B` responds with an answer, which allows `C` to respond 
upstream with the full answer.

Pros:
  * Simple mental model, and no need to have a forward-planned message
  * Explanations have a simple mapping and most easily produce a non-nested structure that aligns with the desired API
    and query structure.
    
Cons:
  * Does not equally load balance message passing among actors: the root conjunction `C` will receive N, send N, receive N
    from `A`, send N to `B`, and receive `N` from B, and send `N` upstream. However, `A` and `B` only receive and send exactly
    `N`.
    This means, that the root processes `T/2` messages (T is the total number of messages), and all other sub-query actors
    together process `T/2` messages. This means the root actor is the message bottleneck.


**This is the model we have selected for the time being, since it leads to much simpler implementations of reasoner, including
explanations. We also cannot confirm whether the message load imbalance will be big enough to matter when other types of work 
will be performed in actors (such as traversals, unification, etc.), and when the computation graph is larger, naturally
distributing work more evenly. Requires real world benchmarking.**


### Pipelined Natural Tree Messaging Model

The middle ground between these two models, which should combine the best of both, is to represent a natural tree model,
but partition the Conjunction `C` into sub-actors, called `C_A` and `C_B`. These interact with actors `A` and `B`.
We add a special "forwarding" message between `C_A` and `C_B`, according to the conjunction's plan. This introduces a
messaging overhead for these uni-directional messages. However, by creating K more actors (for K sub-query actors),
we end up much more evenly load-balancing message processing across more actors.

In other words, each actor will end up processing M/(2K) messages on average, as opposed to M/K messages in the Linearised model,
or at works M/2 messages in the Natural Tree Model.

Pros:
  * equally load balance work message processing across all actors
  * retain, at least outwardly, the natural tree-shaped model
  
Cons:
  * adds a messaging overhead between new conjunction actors
  * will obfuscate model with special non-responded-to messages, and a "triangular" message flow within a conjunction actor
    and its sub-actors.