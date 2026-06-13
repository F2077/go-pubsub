# cmd/quickstart

A runnable, minimal go-pubsub example. The program is the same
five-step recipe shown in the top-level `README.md` under
**Quick Start**:

1. `pubsub.NewBroker[string]()` — create a broker.
2. `pubsub.NewPublisher[string](broker)` + `pubsub.NewSubscriber[string](broker)`
   — bind a publisher and a subscriber to it.
3. `subscriber.Subscribe("alerts", WithChannelSize[string](Medium), WithTimeout[string](200ms))`
   — attach to a topic with a buffered channel and a sliding idle
   timer.
4. `publisher.Publish("alerts", "CPU over 90%!")` — send a message.
5. `select { case msg := <-sub.Ch: ...; case err := <-sub.ErrCh: ... }`
   — consume one message and print it.

The point of this binary is to mirror `README.md`'s Quick Start
verbatim, so a reader can `git clone` the repo and confirm the
documentation by running:

```bash
go run ./cmd/quickstart
```

Expected output:

```
Received: CPU over 90%!
```

The whole program is ~50 lines. It deliberately exercises only
the path a brand-new user cares about: one broker, one publisher,
one subscriber, one topic, one message, one receive. Anything else
(sentinels, capacity errors, multi-topic `Subscribes`, the
sliding-timeout firing naturally, etc.) is left to
`cmd/quickstart-e2e`.

## Layout

`cmd/quickstart/main.go` follows the standard Go layout per
[golang-standards/project-layout](https://github.com/golang-standards/project-layout#cmd-directory):

> Main applications for this project. The directory name for each
> application should match the name of the executable you want to
> have (e.g. `/cmd/myapp`).

A short `package main` comment at the top of `main.go` points
readers at `cmd/quickstart-e2e` for the deeper walk-through.
