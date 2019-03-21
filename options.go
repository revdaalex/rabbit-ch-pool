package rabbit_ch_pool

import "time"

// Options config struct
type Options struct {
	Addr                 string
	PoolSize             int
	ExchangeDeclare      bool
	ExchangeName         string
	ExchangeType         string
	PoolTimeout          time.Duration
}
