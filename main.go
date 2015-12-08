package main

import (
  "github.com/tcooper8/webApp/pman"
  "github.com/tcooper8/webApp/logging"
  "github.com/tcooper8/webApp/commons"
  "github.com/tcooper8/webApp/events"
)

func onAuthRegister(log *logging.Log, bus *commons.EventBus) error {
  handler := make(chan interface{})
  err := bus.On("auth.Register", handler)
  if err != nil {
    return err
  }

  go func() {
    for _ = range(handler) {
      log.Info("Got message!")
    }
  }()

  return nil
}

func main () {
  log := logging.New("main", logging.INFO)

  bus, err := commons.NewEventBus("test")
  if err != nil {
    log.Error("Error creating eventBus: %s", err)
    return
  }

  err = onAuthRegister(log, bus)
  if err != nil {
    log.Error("Unable to start auth listener: %s", err)
  }

  pman := pman.New()
  err = pman.Start("greeter", nil)
  if err != nil {
    log.Info("Error starting process: %s\n", err)
  }
  log.Info("Started process\n")

  bus.Publish("auth.Register", events_auth.Register{
    "Bobby",
    "Singer",
    "bobby@gmail.com",
    "samanddean",
    "1234",
    "Male",
  })

  for { }
}
