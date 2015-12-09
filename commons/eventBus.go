package commons

import (
  "errors"
  "github.com/tcooper8/webApp/logging"
  "github.com/streadway/amqp"
  "code.google.com/p/go-uuid/uuid"
  "time"
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
  uuid          string
  log           *logging.Log
  amqpMap       map[string] Amqp  // Maps eventType -> Amqp
  pqMap         map[string] string // Maps eventType -> privateQueue
  subMap        map[string] SubMapping
  privateQueue  string
  cancelToken   chan bool
  taskMap       map[string] chan interface{}
}

func getSubMap() map[string] SubMapping {
  // todo: Add distributed config to this.
  subMap := make(map[string] SubMapping)
  subMap["auth.Register"] = SubMapping{
    endPoint: "amqp://localhost:5672/",
    eventType: "auth.register",
    exchange: "auth",
    reqQueue: "auth.register.request",
    respListenQueue: "auth.register.response",
  }

  return subMap
}

func NewEventBus (name string) (*EventBus, error) {
  log := logging.New(name, logging.INFO)
  uuid := uuid.New()

  subMap := getSubMap()
  cancelToken := make(chan bool)

  amqpMap := make(map[string] Amqp)
  pqMap := make(map[string] string)
  taskMap := make(map[string] chan interface{})

  eventBus := EventBus{
    uuid:         uuid,
    log:          log,
    amqpMap:      amqpMap,
    pqMap:        pqMap,
    subMap:       subMap,
    cancelToken:  cancelToken,
    taskMap:      taskMap,
  }

  return &eventBus, nil
}

func (bus *EventBus) getAmqp(eventType string) (*Amqp, error) {
  sub, ok := bus.subMap[eventType]
  if !ok {
    return nil, errors.New("Unmapped type")
  }

  endPoint := sub.endPoint
  amq, ok := bus.amqpMap[eventType]
  if ok {
    return &amq, nil
  }

  log := bus.log
  log.Info("Generating amqp connection for %s", eventType)

  conn, err := amqp.Dial(endPoint)
  if err != nil {
    return nil, err
  }

  channel, err := conn.Channel()
  if err != nil {
    return nil, err
  }

  //privateQueue := "amqp" + bus.uuid
  privateQueue, err := channel.QueueDeclare("", false, false, true, false, nil)
  if err != nil {
    return nil, err
  }

  channel2, err := conn.Channel()
  if err != nil {
    return nil, err
  }
  go bus.consumePrivate(privateQueue.Name, channel2)

  bus.pqMap[eventType] = privateQueue.Name

  amq = Amqp{
    conn,
    channel,
  }
  bus.amqpMap[eventType] = amq

  return &amq, nil
}

func (bus *EventBus) Stop() {
  bus.cancelToken <- true
}

type Handle struct {
  Reply   chan<- interface{}
  Msg     interface{}
}

func (bus *EventBus) consumePrivate(privateQueue string, channel *amqp.Channel) {
  log := bus.log

  deliveries, err := channel.Consume(privateQueue, "", true, false, false, false, nil)
  if err != nil {
    log.Error("Cannot setup private queue: %s", err)
    panic("Cannot setup private queue")
  }

  log.Info("Consuming private messages off of %s", privateQueue)
  for {
    select {
    case <-bus.cancelToken:
      log.Info("Stopping private queue %s", bus.uuid)
      return

    case letter := <-deliveries:
      log.Info("Got message on private queue")
      task, ok := bus.taskMap[letter.CorrelationId]
      if !ok {
        log.Info("%s %s %s %s", letter.RoutingKey, letter.Exchange, letter.ContentType, letter.Type)
        log.Warn("Received invalid msg %s", letter)
        continue
      }

      //letter.Ack(false)

      task <- "Got it!"

    default:
      continue
    }
  }
}

func (bus *EventBus) On(eventType string, handler chan<- *Handle) error {
  log := bus.log

  var err error = nil

  sub, ok := bus.subMap[eventType]
  if !ok {
    return errors.New("Event type is unmapped")
  }

  amqp, err := bus.getAmqp(eventType)
  channel := amqp.channel

  if err != nil {
    return err
  }

  log.Info("Creating exchange %s for event %s", sub.exchange, eventType)
  err = channel.ExchangeDeclare(sub.exchange, "topic", true, false, false, false, nil)
  if err != nil {
    return err
  }

  log.Info("Creating request queue %s for event %s", sub.reqQueue, eventType)
  _, err = channel.QueueDeclare(sub.reqQueue, false, false, false, false, nil)
  if err != nil {
    return err
  }

  log.Info("Binding the queues to the appropriate routing keys")
  err = channel.QueueBind(sub.reqQueue, eventType, sub.exchange, false, nil)
  if err != nil {
    return err
  }

  deliveries, err := channel.Consume(sub.reqQueue, "", false, false, false, false, nil)
  if err != nil {
    return err
  }

  err = channel.Qos(1, 0, false)
  if err != nil {
    return err
  }

  // Spin up the goroutine for spitting values to the handler.
  go func() {
    log.Info("Consuming messages from %S", sub.reqQueue)

    msgDecrementer := make(chan int)
    activeMsgs := 0

    for {
      select {
      case <-bus.cancelToken:
        log.Info("Stopping %s", bus.uuid)
        return

      case <-msgDecrementer:
        activeMsgs -= 1

      case letter := <-deliveries:
        reply := make(chan interface{})
        handle := Handle{
          reply,
          "Hello!",
        }
        handler <- &handle
        activeMsgs += 1

        go func() {
          select {
          case resp := <-reply:
            log.Info("Publishing response %s", resp)

            msgDecrementer <- 1
            err := bus.respond(channel, &letter, resp)
            if err != nil {
              log.Warn("Encountered error: %s", err)
            }
            letter.Ack(false)
          }
        }()
      }

      // Sleep until msgs are done.
      for activeMsgs >= 32 {
        time.Sleep(16 * time.Millisecond)
      }
    }
  }()

  return nil
}

func (bus *EventBus) respond(channel *amqp.Channel, letter *amqp.Delivery, resp interface{}) error {
  log := bus.log

  log.Info("Publishing response to %s %s", letter.ReplyTo, letter.CorrelationId)
  err := channel.Publish(
    "",
    letter.ReplyTo,
    false,
    false,
    amqp.Publishing{
      ContentType:    "text/plain",
      Body:           []byte("{}"),
      CorrelationId:  letter.CorrelationId,
    },
  )
  return err
}

func (bus *EventBus) Publish(eventType string, msg interface{}) (interface{}, error) {
  log := bus.log

  amq, err := bus.getAmqp(eventType)
  if err != nil {
    return nil, err
  }

  channel := amq.channel
  privateQueue := bus.pqMap[eventType]

  sub, ok := bus.subMap[eventType]
  if !ok {
    return nil, errors.New("Event type is unmapped")
  }

  // Add this task to the map.
  correlationId := uuid.New()
  task := make(chan interface{})
  bus.taskMap[correlationId] = task

  log.Info("Requesting with %s %s", privateQueue, correlationId)
  err = channel.Publish(
    sub.exchange,
    eventType,
    false,
    false,
    amqp.Publishing{
      ContentType:    "text/plain",
      Body:           []byte("{ }"),
      ReplyTo:        privateQueue,
      CorrelationId:  correlationId,
    },
  )
  if err != nil {
    delete(bus.taskMap, correlationId)
    return nil, err
  }

  timeout := make(chan bool, 1)
  go func () {
    time.Sleep(20 * time.Second)
    timeout <- true
  }()

  select {
  case <-timeout:
    return nil, errors.New("Timeout exception")
  case reply := <-task:
    return reply, nil
  }
}
