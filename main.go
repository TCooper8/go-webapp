package main

import (
	"github.com/tcooper8/go-webapp/commons"
	"github.com/tcooper8/go-webapp/events"
	"github.com/tcooper8/go-webapp/logging"
)

var doneChan = make(chan int)

func onAuthRegister(log *logging.Log, bus *commons.EventBus) error {
	handler := make(chan *commons.Handle)

	go func() {
		for handle := range handler {
			log.Info("Got message!")
			handle.Reply <- "Hello!"

			doneChan <- 1
		}
	}()

	return bus.On("auth.Register", handler)
}

func main() {
	log := logging.New("main", logging.INFO)

	bus, err := commons.NewEventBus("test")
	if err != nil {
		log.Error("Error creating eventBus: %s", err)
		return
	}

	err = onAuthRegister(log, bus)
	if err != nil {
		log.Error("Unable to start auth listener: %s", err)
		panic("")
	}

	for i := 0; i < 10000; i++ {
		msg, err := bus.Publish("auth.Register", events_auth.Register{
			"Bobby",
			"Singer",
			"bobby@gmail.com",
			"samanddean",
			"1234",
			"Male",
		})
		if err != nil {
			log.Error("Publish error: %s", err)
		}
		log.Info("Got message response %s!", msg)

		<-doneChan
	}

	log.Info("Send!")
}
