package rabbit_ch_pool

import (
	"github.com/streadway/amqp"
	"sync"
	"time"
)

// Pooler pool interface
type Pooler interface {
	Get() (*amqp.Channel, error)
	Put(ch *amqp.Channel) error
}

// ChannelPool pool struct
type ChannelPool struct {
	conn            *amqp.Connection
	chMu            sync.Mutex
	chPool          chan *amqp.Channel
	exchangeDeclare bool
	exchangeName    string
	exchangeType    string
	poolSize        int
	poolTimeout     time.Duration
}

// NewChannelPool create new pool with amqp channels
func NewChannelPool(opt *Options, conn *amqp.Connection) *ChannelPool {
	p := &ChannelPool{
		conn:        conn,
		poolSize:    opt.PoolSize,
		poolTimeout: opt.PoolTimeout,
	}

	if opt.PoolTimeout == 0 {
		p.poolTimeout = 10
	}

	if opt.PoolSize == 0 {
		p.chPool = make(chan *amqp.Channel, 5)
	} else {
		p.chPool = make(chan *amqp.Channel, opt.PoolSize)
	}

	p.clearPool()

	for i := 0; i < p.poolSize; i++ {
		err := p.spawnChannel()
		if err != nil {
			return nil
		}
	}

	return p
}

func (p *ChannelPool) spawnChannel() error {
	ch, err := p.conn.Channel()
	if err != nil {
		return err
	}

	if p.exchangeDeclare {
		err = ch.ExchangeDeclare(
			p.exchangeName,
			p.exchangeType,
			false,
			false,
			false,
			false,
			nil,
		)
		if err != nil {
			return err
		}
	}

	p.chMu.Lock()
	if len(p.chPool) < p.poolSize {
		p.chPool <- ch
	} else {
		ch.Close()
	}
	p.chMu.Unlock()

	return nil
}

func (p *ChannelPool) _getChannel() (*amqp.Channel, error) {
	timeout := time.After(p.poolTimeout * time.Second)
loop:
	for {
		select {
		case rCh := <-p.chPool:
			err := rCh.Confirm(false)
			if err == nil {
				return rCh, nil
			}
			if err == amqp.ErrClosed {
				err = p.spawnChannel()
				if err != nil {
					break loop
				}
				break
			} else {
				return nil, amqp.ErrClosed
			}
		case <-timeout:
			break loop
		}
	}

	return nil, amqp.ErrClosed
}

func (p *ChannelPool) _releaseChannel(ch *amqp.Channel) error {
	err := ch.Confirm(false)
	if err != nil {
		err = p.spawnChannel()
		return err
	}

	p.chMu.Lock()
	if len(p.chPool) < p.poolSize {
		p.chPool <- ch
	} else {
		ch.Close()
	}
	p.chMu.Unlock()

	return nil
}

func (p *ChannelPool) clearPool() {
	if len(p.chPool) != 0 {
	L:
		for {
			select {
			case <-p.chPool:
			default:
				break L
			}
		}
	}
}

// Get get channel from pool
func (p *ChannelPool) Get() (*amqp.Channel, error) {
	return p._getChannel()
}

// Put put channel to pool
func (p *ChannelPool) Put(ch *amqp.Channel) error {
	return p._releaseChannel(ch)
}
