package commons

import (
  "github.com/tcooper8/webApp/logging"
  "github.com/streadway/amqp"
)

type EventMapping struct {
  endPoint    string
  eventType   string
  exchange    string
  reqQueue    string
  respListenQueue string
}

type change struct {
  msg     interface{}
  sender  chan<- error
}

type amqpConn struct {
  conn  *amqp.Connection
  ch    *amqp.Channel
}

type AmqpPool struct {
  log           *logging.Log
  eventMap      map[string] *EventMapping
  privateQueues map[string] string
  connMap       map[string] *amqpConn
  cancelToken   chan bool
  taskMap       map[string] chan interface{}
  changes       chan interface{}
}

func NewAmqpPool(name string) (*AmqpPool, error) {
  log := logging.New(name + ":Amqp", logging.INFO)
  eventMap := make(map[string] *EventMapping)
  privateQueues := make(map[string] string)
  connMap := make(map[string] *amqpConn)

  cancelToken := make(chan bool)
  taskMap := make(map[string] chan interface{})
  changes := make(chan interface{}, 16)

  pool := AmqpPool{
    log,
    eventMap,
    privateQueues,
    connMap,
    cancelToken,
    taskMap,
    changes,
  }

  do pool.handleChanges()
}

func (pool *AmqpPool) handleChanges() {
  log := pool.log
  changes := pool.changes

  var err error
  var msg interface{}
  var sender chan<- error

  for c = range changes {
    msg = c.msg
    sender = c.sender

    switch msg := msg.(type) {
    default:
      log.Warn("Received unexpected change: %T as %s", msg, msg)

    case *EventMapping:
      // Update to the event mapping.
      sender <- pool.updateMapping(msg)
    }
  }
}

func ListenOn(eventType string, handler chan<- interface{}) error {

}

func (pool *AmqpPool) updateMapping(mapping *EventMapping) error {

}

func (pool *AmqpPool) UpdateMapping(mapping *EventMapping) error {
  sender := make(chan error, 1)
  pool.changes <- change{
    mapping,
    sender,
  }

  err := <-sender
  close(sender)
  return err
}
