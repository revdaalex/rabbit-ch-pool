package rabbit_ch_pool

import (
	"errors"
	"github.com/streadway/amqp"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const (
	// When reconnecting to the server after connection failure
	reconnectDelay = 5 * time.Second
)

var (
	ErrorNotConnected = errors.New("failed: not connected")
)

// Rabbit service struct
type Rabbit struct {
	log        *log.Logger
	opt        *Options
	connection *amqp.Connection
	chPool     Pooler
	sigs       chan os.Signal
	isReady    bool
}

// NewRabbit create new svc
func NewRabbit(opt *Options) *Rabbit {
	r := &Rabbit{
		log:  log.New(os.Stdout, "", log.LstdFlags),
		opt:  opt,
		sigs: make(chan os.Signal, 1),
	}
	conn, _ := r.connect()
	r.connection = conn
	signal.Notify(r.sigs, syscall.SIGINT, syscall.SIGTERM)
	go r.handleReconnect()

	return r
}

func (r *Rabbit) handleReconnect() {
	for {
		if r.connection.IsClosed() {
			r.isReady = false
			log.Println("Attempting to connect")

			_, err := r.connect()

			if err != nil {
				log.Println("Failed to connect. Retrying...")

				select {
				case <-r.sigs:
					return
				case <-time.After(reconnectDelay):
				}
				continue
			}
		}
		time.Sleep(reconnectDelay)
	}
}

func (r *Rabbit) connect() (*amqp.Connection, error) {
	r.isReady = false
	conn, err := amqp.Dial(r.opt.Addr)

	if err != nil {
		return nil, err
	}

	r.changeConnection(conn)
	r.isReady = true
	log.Println("Connected!")
	return conn, nil
}

func (r *Rabbit) changeConnection(connection *amqp.Connection) {
	r.connection = connection
	r.chPool = NewChannelPool(r.opt, r.connection)
}

// PublishMessage publish message to queue
func (r *Rabbit) PublishMessage(body, routingKey string) error {
	if !r.isReady {
		return ErrorNotConnected
	}
	ch, err := r.chPool.Get()
	if err != nil {
		return err
	}

	err = ch.Publish(
		r.opt.ExchangeName,
		routingKey,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		})
	if err != nil {
		return err
	}

	r.log.Printf("Sent %s", body)

	err = r.chPool.Put(ch)
	if err != nil {
		return err
	}

	return nil
}
