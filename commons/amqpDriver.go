package commons

import (
  "code.google.com/p/go-uuid/uuid"
  "github.com/tcooper8/webApp/logging"
  "github.com/streadway/amqp"
)

type AmqpEventMapping struct {
  endPoint    string
  eventType   string
  exchange    string
  reqQueue    string
  respListenQueue string
  privateQueue    string
}

type connInfo struct {
  conn    *amqp.Connection
  ch      *amqp.Channel
  mapping *AmqpEventMapping
}

type AmqpPool struct {
  log           *logging.Log
  privateQueues map[string] string
  connMap       map[string] *connInfo
  cancelToken   chan bool
  taskMap       map[string] chan interface{}
  changes       chan interface{}
}

type setMapping struct {
  mapping *AmqpEventMapping
}

type getConnMsg struct {
  eventType string
}

type getConnRes struct {
  conn  *connInfo
  ok    bool
}

type change struct {
  msg   interface{}
  reply chan<- interface{}
}

func NewAmqpPool(name string) (*AmqpPool, error) {
  log := logging.New(name + ":Amqp", logging.INFO)
  privateQueues := make(map[string] string)
  connMap := make(map[string] *connInfo)

  cancelToken := make(chan bool)
  taskMap := make(map[string] chan interface{})
  changes := make(chan interface{}, 16)

  pool := AmqpPool{
    log,
    privateQueues,
    connMap,
    cancelToken,
    taskMap,
    changes,
  }

  pool.loadDefaultMappings()

  do pool.handleChanges()

  return &pool, nil
}

func (pool *AmqpPool) loadDefaultMappings() {
  pool.setMapping(
    AmqpEventMapping{
      endPoint: "amqp://localhost:5672/",
      eventType: "auth.register",
      exchange: "auth",
      reqQueue: "auth.register",
      privateQueue: uuid.New(),
    }
  )
}

func (pool *AmqpPool) handleChanges() {
  log := pool.log
  changes := pool.changes

  var err error
  var msg interface{}
  var sender chan<- error

  for c = range changes {
    msg = c.msg

    switch msg := msg.(type) {
    default:
      log.Warn("Received unexpected change: %T as %s", msg, msg)

    case *AmqpEventMapping:
      // Update to the event mapping.
      pool.setMapping(msg)

    case getMappingMsg:
      c.reply <- pool.getMapping(msg.eventType)
  }
}

func (pool *AmqpPool) RespondOn(eventType string, handler chan<- interface{}) error {
  var err error = nil
  log := pool.log

  connInfo, ok := pool.GetConnInfo(eventType)
  if !ok {
    return errors.New(fmt.Sprintf(
      "Event type %s is unmapped",
      eventType,
    ))
  }

  channel := connPair.channel

  err = channel.ExchangeDeclare(
}

func (pool *AmqpPool) getConn(eventType string) (*connInfo, bool) {
  return pool.connMap[eventType]
}

func (pool *AmqpPool) GetMapping(eventType string) (*AmqpEventMapping, bool) {
  reply := make(chan interface{}, 1)
  pool.changes <- change{
    msg: getConnMsg{
      eventType,
    }
  }

  _res := <-reply
  close(reply)

  res := _res.(getConnRes)
  return res.conn.mapping, res.ok
}

func (pool *AmqpPool) setMapping(mapping *AmqpEventMapping) error {
  // First, creating the connections, etc...

  eventType := mapping.eventType
  endPoint := mapping.endPoint

  conn, err := amqp.Dial(endPoint)
  if err != nil {
    return err
  }

  channel, err := conn.Channel()
  if err != nil {
    return err
  }

  amq := amqpConn{
    conn,
    channel,
    mapping,
  }

  // Update the maps.
  pool.connMap[eventType] = amq

  return nil
}

func (pool *AmqpPool) SetMapping(mapping *AmqpEventMapping) {
  pool.changes <- change{
    msg: setMapping{
      mapping,
    }
  }
}

























































































