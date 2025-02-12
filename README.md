# Concurrency

The LAZY File Manager simulates concurrent file access using threads, locks, and semaphores. Users request READ, WRITE, and DELETE operations, but only a limited number can access a file at once. Requests wait in a queue, and users cancel if their request isn't processed within a set time (T seconds).
## Concurrency Rules:

    READ: Multiple users can read a file at the same time, even during a write.
    WRITE: Only one user can write at a time.
    DELETE: Only possible if no active reads/writes; once deleted, the file is gone.

## Simulation Workflow:

    Users submit requests with an operation and a file.
    LAZY waits 1 second before processing a request.
    If a request exceeds T seconds, the user cancels it.
    Requests are processed in order, following concurrency constraints.
    Output logs events: requests, processing start, completions, cancellations, and system sleep.
