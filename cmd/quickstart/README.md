# cmd/quickstart

A runnable, end-to-end example of `go-pubsub`. One `main` function that
wires up a broker, a publisher, and a subscriber on the same process
and delivers a single message through the pipeline.

## Run it

```bash
# from the repo root
go run ./cmd/quickstart

# or, once the module is tagged
go run github.com/F2077/go-pubsub/cmd/quickstart@latest
```

Expected output (one line):

```
Received: CPU over 90%!
```

If the subscriber's idle timer elapses before the publisher's goroutine
schedules, you'll see `Error: subscription timeout` instead — that's
the `WithTimeout(5*time.Second)` sliding-deadline working as designed.

## What it shows

| Step | API surface exercised |
| ---: | --- |
| 1 | `pubsub.NewBroker[string]()` — generic broker constructor |
| 2 | `pubsub.NewPublisher[string]` / `pubsub.NewSubscriber[string]` |
| 3 | `pubsub.WithChannelSize[string](pubsub.Medium)` |
| 3 | `pubsub.WithTimeout[string](5*time.Second)` |
| 4 | `sub.Ch` (read-only message channel) |
| 4 | `sub.ErrCh` (read-only error channel) |
| 5 | `defer sub.Close()` — release the subscription |

## Layout rationale

`cmd/quickstart/main.go` follows the standard Go layout per
[golang-standards/project-layout](https://github.com/golang-standards/project-layout#cmd-directory):

> Main applications for this project. The directory name for each
> application should match the name of the executable you want to have
> (e.g., `/cmd/myapp`). It's common to have a small `main` function
> that imports and invokes the code from the `/internal` and `/pkg`
> directories and nothing else.

The example deliberately keeps the `main` package under 50 lines so
the API surface stays the focus — there is no goroutine pool, no
graceful-shutdown handler, and no signal trapping, because those are
application concerns, not library concerns.

## What it does NOT show

- **Publisher→Broker errors.** `Publish` can return
  `pubsub.ErrSubscriptionCapacityExceeded` if the broker is at capacity.
  In this example we ignore the error (`_ = publisher.Publish(...)`)
  for brevity; production code should check it.
- **Subscriber close.** `defer sub.Close()` is the only cleanup. A
  long-running program would also call `subscriber.Close()` to release
  every topic the subscriber is on, not just one.
- **Channel size = 0 (`Block`)** mode. The quickstart uses `Medium` to
  stay drop-tolerant; switch to `pubsub.Block` if you want publish to
  block until the consumer reads.
