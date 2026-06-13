# cmd/quickstart

A runnable, end-to-end walk-through of the entire `go-pubsub` public
surface. The program is structured as 13 numbered phases: each phase
exercises one or more exported APIs, prints a header, and then continues
to the next. The whole script runs in well under a second.

## Run it

```bash
# from the repo root
go run ./cmd/quickstart

# or, once the module is tagged
go run github.com/F2077/go-pubsub/cmd/quickstart@latest
```

A successful run prints a transcript that starts with `=== 1. Brokers ===`
and ends with `quickstart: ok`. The full output is ~30 lines and fits in a
terminal scrollback.

## What it shows

| Phase | APIs exercised |
| ---: | --- |
| 1 | `pubsub.NewBroker[T]`, `Broker.String`, `Broker.Capacity`, `pubsub.DefaultCapacity` |
| 2 | `pubsub.WithLogger`, `pubsub.WithId`, `pubsub.ErrLoggerNil`, `pubsub.ErrBrokerIdEmpty`, `errors.Is` |
| 3 | `pubsub.NewPublisher[T]`, `pubsub.NewSubscriber[T]`, `Publisher.String`, `Publisher.Id`, `Subscriber.String`, `Subscriber.Id` |
| 4 | `Subscriber.Subscribes`, `Subscription.Ch` (capacity), `Subscription.ErrCh` (lazy-nil contract), `Subscription.OnClose` |
| 5 | `Subscriber.Subscribe`, `pubsub.WithChannelSize(pubsub.Block)`, `pubsub.WithTimeout`, `pubsub.DefaultTimeout` (implicit), `cap` on receive-only channel |
| 6 | (helper plumbing — drain goroutines) |
| 7 | `Publisher.Publish` × 15 across 3 topics / 2 publishers |
| 8 | `pubsub.WithCapacity(2)`, `pubsub.ErrSubscriptionCapacityExceeded`, `errors.Is` |
| 9 | `pubsub.ErrSubscriptionTimeout` (the sliding 400 ms timer fires naturally) |
| 10 | `Subscription.Close` (fires `OnClose`), `Subscriber.Close` (idempotent + `ErrSubscriberClosed`), `Subscriber.Subscribe` after `Close` returns `ErrSubscriberClosed` |
| 11 | Per-subscription `sub.Close()` × 3 (each fires its own `OnClose`) |
| 12 | (joins the 4 drain goroutines) |
| 13 | `Broker.Topics` — may show a non-empty snapshot briefly after Close due to asynchronous topic reaping |

The script's `main` package keeps the orchestration in `run()` and the
drain logic in a generic `drainSubscription[T]` helper. `main` itself is
a thin error wrapper that prints failures to stderr and exits non-zero.

## Layout rationale

`cmd/quickstart/main.go` follows the standard Go layout per
[golang-standards/project-layout](https://github.com/golang-standards/project-layout#cmd-directory):

> Main applications for this project. The directory name for each
> application should match the name of the executable you want to have
> (e.g., `/cmd/myapp`). It's common to have a small `main` function
> that imports and invokes the code from the `/internal` and `/pkg`
> directories and nothing else.

The example uses ~260 lines of source so that the runnable program can
demonstrate every exported symbol in one go (constructors, options,
constants, sentinels, methods, fields, callbacks). All of the API
surface that is *not* observable from a single-process script — the
genuinely cross-process pieces — is intentionally left out, and
`go-pubsub` is an in-process library, so this covers the whole product.

## What it does NOT show

- **Backpressure / guaranteed delivery.** The library is fire-and-forget
  by design: `Publish` is non-blocking, full channels drop messages,
  and there is no acknowledgement path. The quickstart only uses
  `Block` once (in phase 5) for the sliding-timeout demo; everywhere
  else it uses the drop-tolerant `Medium` buffer.
- **Signal handling / graceful shutdown.** A long-running program
  would also trap `SIGINT`/`SIGTERM` and call `subscriber.Close()` on
  exit. The quickstart exits immediately after `run()` returns, so
  there is nothing to trap.
- **Subscriber error paths beyond the sentinels.** `subscriber.Close()`
  on a multi-topic subscriber is exercised in phases 10–11; the
  per-subscription `sub.Close()` after the subscriber itself has been
  closed is also exercised. Other close-order edge cases (e.g. closing
  the same sub twice) are covered by the unit tests in `pubsub/`.

## Why the broker's `Topics()` snapshot may be non-empty at exit

Phases 10–11 close every subscription, but the broker reaps empty
topics in a separate goroutine to avoid a `subscription → broker`
lock-order deadlock (see `pubsub/broker.go:285`). A freshly-emptied
topic may therefore still appear in the `Topics()` snapshot for a
short window after `subscriber.Close()` returns. This is intentional,
documented behaviour, not a leak.
