

## Explanation Guarantees and Behaviour

Resolution is the term for a more complete data structure form which Explanation (user-facing) are taken from. Explanations
are abbreviated Resolution trees.

Resolutions are recorded via an actor, and grow as answers are deduplicated.
 
This means that explanations may be incomplete or change as the user requests them and more answer processing occurs.
Further answer processing may be initiated by the user as well as our precompute buffer.


Basic Guarantees:
1. A valid but possibly incomplete explanation tree will be available when the user requests it from the client side.
2. An explanation tree will be complete if the user consumes all answers of a query. This requires a query without a `match...limit`.

Interaction behaviour:
1. When a user receives an answer, the explanation tree retrieved for that answer may grow due to precompute. To prevent
 the explanation tree from growing beyond the user's answer processing, the user should disable precompute (this feature
 may not exist yet).
