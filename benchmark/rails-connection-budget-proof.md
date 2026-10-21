# Selected-pool checkout proof — Story 06

Let P = pipeline_capacity, R = resolve_concurrency (including its default of
ingest_concurrency), and I = ingest_concurrency. Acp requires
**min(P, R + I) + 1** connections in its selected Rails transaction pool.

1. An admitted cycle owns one pipeline reservation before cursor initialization
   or resolution. Both callbacks acquire the same resolution semaphore, bounded
   by min(P, R). They run sequentially within a cycle.
2. Resolution returns detached values and exits both `with_connection` and the
   Rails executor before fetch starts. Fetch/network waits, cursor-store reads
   and writes, ingestion queue/reply waits, and progress acknowledgement/backoff
   hold no Acp-owned selected-pool checkout.
3. Ingestion runs on a fixed worker set bounded by min(P, I). A cycle waits for
   its ingestion callback and transaction to unwind before acknowledging,
   retrying, or releasing its reservation. It cannot resolve concurrently with
   its own ingestion. Thus each runnable cycle accounts for at most one checkout,
   and resolution plus ingestion use at most min(P, R + I) connections.
4. Story 02 retry isolation can retain P + retry_capacity cycle tasks, but only P
   own runnable pipeline reservations. Fetch/ingest failures unwind their callback
   and checkout before parking. Parked cycles hold no checkout; readmission must
   reacquire a pipeline reservation before retrying. Ingestion cancellation joins
   the callback before releasing the reservation. Extra retained cycles therefore
   do not increase the database bound.
5. Initial discovery precedes cycle admission. Periodic discovery has one task;
   Story 03 pages execute sequentially and materialize detached IDs inside their
   checkout before Redis registration. Legacy emitter discovery can retain its
   connection across registration, but still holds only one. Conservatively add
   one discovery connection, including at startup.
6. Database callbacks use `with_connection(prevent_permanent_checkout: true)`
   inside the Rails executor. Calling Active Record's `connection` or
   `lease_connection` within that scope does not retain the selected checkout on
   return. Exceptions, cancellation, and lease-loss cancellation unwind these
   scopes, including ingestion rollback and executor completion hooks.

This is an **Acp-owned** bound under the detached-data, cooperative-cancellation
callback contract. Nested consumer thread/fiber fanout, application-created
checkouts, web requests, jobs, other pools, and callbacks retaining resources or
starting work outside their scope need separate budgets. An ambient application
checkout is not returned by Acp; account for it as application headroom.

Revalidate this proof and the exact-bound PostgreSQL overlap/lifecycle tests
before changing retry admission, ingestion cancellation/join ordering, discovery
page concurrency, or callback checkout/executor scopes. Any new database-capable
cycle must share P's admission bound, or the formula must change. Parallel
discovery pages likewise require a revised discovery budget or shared admission.
