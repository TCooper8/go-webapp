package commons

import (
  "errors"
  "github.com/tcooper8/webApp/logging"
  "github.com/streadway/amqp"
  "code.google.com/p/go-uuid/uuid"
)

type SubMapping struct {
  endPoint    string
  eventType   string
  exchange    string
  reqQueue    string
  respListenQueue string
}

type Amqp struct {
  conn    *amqp.Connection
  channel *amqp.Channel
}

type EventBus struct {
  uuid    string
  log     *logging.Log
  amqpMap  map[string] Amqp
  pqMap   map[string] string
  subMap  map[string] SubMapping
  privateQueue  string
  cancelToken   chan bool
}

func getSubMap(endPoint string) map[string] SubMapping {
  // todo: Add distributed config to this.
  subMap := make(map[string] SubMapping)
  subMap["auth.Register"] = SubMapping{
    endPoint: "amqp://localhost:5672/",
    eventType: "auth.Register",
    exchange: "auth",
    reqQueue: "auth.login.request",
    respListenQueue: "auth.login.response",
  }

  return subMap
}

func NewEventBus (name string) (*EventBus, error) {
  log := logging.New(name, logging.INFO)
  uuid := uuid.New()

  endPoint := "amqp://localhost:5672/"

  log.Info("Connecting to eventBus %s", endPoint)
  conn, err := amqp.Dial(endPoint)
  if err != nil {
    return nil, err
  }

  //log.Info("Connected. Creating channel for %s", endPoint)
  //channel, err := conn.Channel()
  //if err != nil {
  //  return nil, err
  //}

  //log.Info("Channel created.")

  //// Setup the private queue.
  //privateQueue := name + uuid
  //_, err = channel.QueueDeclare(privateQueue, false, true, true, false, nil)
  //if err != nil {
  //  return nil, err
  //}

  subMap := getSubMap(endPoint)
  cancelToken := make(chan bool)

  amqpMap := make(map[string] Amqp)
  pqMap := make(map[string] string)

  eventBus := EventBus{
    uuid:         uuid,
    log:          log,
    amqpMap:      amqpMap,
    pqMap:        pqMap,
    subMap:       subMap,
    cancelToken:  cancelToken,
  }

  return &eventBus, nil
}

func (bus *EventBus) getAmqp(eventType string) (*Amqp, error) {
  amq, ok := bus.amqpMap[eventType]
  if ok {
    return &amq, nil
  }

  conn, err := amqp.Dial(endPoint)
  if err != nil {
    return nil, err
  }

  channel, err := conn.Channel()
  if err != nil {
    return nil, err
  }

  _, err = channel.QueueDeclare(bus.uuid, false, true, true, false, nil)
  if err != nil {
    return nil, err
  }

  amq = Amqp{
    conn,
    channel,
  }

  return &amq, nil
}

func (bus *EventBus) Stop() {
  bus.cancelToken <- true
}

func (bus *EventBus) On(eventType string, handler chan<- interface{}) error {
  log := bus.log

  var err error = nil

  sub, ok := bus.subMap[eventType]
  if !ok {
    return errors.New("Event type is unmapped")
  }

  amqp, err := bus.getAmqp(sub.endPoint)
  channel := amqp.channel

  if err != nil {
    return err
  }

  log.Info("Creating exchange %s for event %s", sub.exchange, eventType)
  err = channel.ExchangeDeclare(sub.exchange, "topic", true, false, false, false, nil)
  if err != nil {
    return err
  }

  log.Info("Creating listener queue %s for event %s", sub.respListenQueue, eventType)
  _, err = channel.QueueDeclare(sub.respListenQueue, false, true, true, false, nil)
  if err != nil {
    return err
  }

  log.Info("Creating request queue %s for event %s", sub.reqQueue, eventType)
  _, err = channel.QueueDeclare(sub.reqQueue, false, true, true, false, nil)
  if err != nil {
    return err
  }

  log.Info("Binding the queues to the appropriate routing keys")
  err = channel.QueueBind(sub.reqQueue, eventType, sub.exchange, false, nil)
  if err != nil {
    return err
  }

  deliveries, err := channel.Consume(sub.reqQueue, bus.uuid, false, false, false, false, nil)
  if err != nil {
    return err
  }

  // Spin up the goroutine for spitting values to the handler.
  go func() {
    for {
      select {
      case <-bus.cancelToken:
        log.Info("Stopping %s", bus.uuid)
        return

      case letter := <-deliveries:
        log.Info("Got a message, woo!!")
        if &letter != nil {
        }
      }
    }
  }()

  return nil
}

func (bus *EventBus) Publish(eventType string, msg interface{}) error {
  channel := bus.channel
  amq, err := bus.getAmqp(
  //log := bus.log

  sub, ok := bus.subMap[eventType]
  if !ok {
    return errors.New("Event type is unmapped")
  }

  var err error = nil

  err = channel.Publish(
    sub.exchange,
    eventType,
    true,
    false,
    amqp.Publishing{
      Headers:        amqp.Table{},
      ContentType:    "text/json",
      ContentEncoding: "",
      Body:           []byte("Hello!"),
      DeliveryMode:   amqp.Transient,
      Priority:       0,
    },
  )

  return err
}
