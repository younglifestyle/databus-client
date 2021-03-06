package databus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-redis/redis/v8"
	log "github.com/sirupsen/logrus"
	"sync"
	"sync/atomic"
	"time"
)

type dial func() (redis.Conn, error)

// Config databus config.
type Config struct {
	Key          string
	Secret       string
	Group        string
	Topic        string
	Action       string // shoule be "pub" or "sub" or "pubsub"
	Buffer       int
	Name         string // redis name, for trace
	Proto        string
	Addr         string
	Auth         string
	PoolSize     int
	Idle         int // pool
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	IdleTimeout  time.Duration
}

const (
	_family     = "databus"
	_actionSub  = "sub"
	_actionPub  = "pub"
	_actionAll  = "pubsub"
	_cmdPub     = "set"
	_cmdSub     = "mget"
	_authFormat = "%s:%s@%s/topic=%s&role=%s"
	_open       = int32(0)
	_closed     = int32(1)
)

var (
	// ErrAction action error.
	ErrAction = errors.New("action unknown")
	// ErrFull chan full
	ErrFull = errors.New("chan full")
	// ErrNoInstance no instances
	ErrNoInstance = errors.New("no databus instances found")
)

// Message Data.
type Message struct {
	Key       string          `json:"key"`
	Value     json.RawMessage `json:"value"`
	Topic     string          `json:"topic"`
	Partition int32           `json:"partition"`
	Offset    int64           `json:"offset"`
	Timestamp int64           `json:"timestamp"`
	d         *Databus
}

// Commit ack message.
func (m *Message) Commit() (err error) {
	m.d.lock.Lock()
	if m.Offset >= m.d.marked[m.Partition] {
		m.d.marked[m.Partition] = m.Offset
	}
	m.d.lock.Unlock()
	return nil
}

// Databus databus struct.
type Databus struct {
	conf *Config

	d   dial
	rdc *redis.Client

	msgs   chan *Message
	lock   sync.RWMutex
	marked map[int32]int64
	idx    int64

	closed int32
}

// New new a databus.
func New(c *Config) *Databus {
	if c.Buffer == 0 {
		c.Buffer = 1024
	}
	d := &Databus{
		conf:   c,
		msgs:   make(chan *Message, c.Buffer),
		marked: make(map[int32]int64),
		closed: _open,
	}

	// new pool
	d.rdc = d.redisPool(c)

	if c.Action == _actionSub || c.Action == _actionAll {

		go d.subproc()
	}
	if c.Action == _actionPub || c.Action == _actionAll {

	}
	return d
}

func (d *Databus) redisPool(c *Config) *redis.Client {
	return redis.NewClient(&redis.Options{
		//????????????
		Addr:     c.Addr,
		Password: fmt.Sprintf(_authFormat, d.conf.Key, d.conf.Secret, d.conf.Group, d.conf.Topic, d.conf.Action),
		//fmt.Sprintf("%s:%s@%s/topic=%s&role=%s", "key", "value", "example", "test1", "sub"), //??????

		//????????????????????????????????????
		PoolSize:     c.PoolSize, // ???????????????socket?????????????????????4???CPU?????? 4 * runtime.NumCPU
		MinIdleConns: c.Idle,     //????????????????????????????????????Idle????????????????????????idle?????????????????????????????????????????????

		//??????
		DialTimeout:  c.DialTimeout,  //?????????????????????????????????5??????
		ReadTimeout:  c.ReadTimeout,  //??????????????????3?????? -1?????????????????????
		WriteTimeout: c.WriteTimeout, //?????????????????????????????????
		//????????????????????????IdleTimeout???MaxConnAge
		IdleTimeout: c.IdleTimeout, //?????????????????????5?????????-1??????????????????????????????
	})
}

func (d *Databus) subproc() {
	var (
		err      error
		r        string
		res      []string
		c        = d.rdc
		commited = make(map[int32]int64)
		commit   = make(map[int32]int64)
	)
	for {
		if atomic.LoadInt32(&d.closed) == _closed {
			if c != nil {
				c.Close()
			}
			close(d.msgs)
			return
		}

		d.lock.RLock()
		for k, v := range d.marked {
			if commited[k] != v {
				commit[k] = v
			}
		}
		d.lock.RUnlock()

		if len(commit) != 0 {
			cmders, err := c.Pipelined(context.TODO(), func(pipeliner redis.Pipeliner) error {
				for k, v := range commit {
					pipeliner.Do(context.TODO(), _cmdPub, k, v)
				}
				return nil
			})
			if err != nil {
				log.Errorf("group(%s) pipeline(SET) commit error(%v)", d.conf.Group, err)
				continue
			}

			for _, cmder := range cmders {
				delete(commit, cmder.Args()[1].(int32))
				commited[cmder.Args()[1].(int32)] = cmder.Args()[2].(int64)
			}
		}
		//for k, v := range commit {
		//	if err = c.Do(context.TODO(), _cmdPub, k, v).Err(); err != nil {
		//		log.Errorf("group(%s) conn.Do(SET,%d,%d) commit error(%v)", d.conf.Group, k, v, err)
		//		break
		//	}
		//	delete(commit, k)
		//	commited[k] = v
		//}
		//if err != nil {
		//	continue
		//}

		// pull messages
		if res, err = c.Do(context.TODO(), _cmdSub, "").StringSlice(); err != nil {
			log.Errorf("group(%s) conn.Do(MGET) error(%v)", d.conf.Group, err)
			continue
		}
		for _, r = range res {
			msg := &Message{d: d}
			if err = json.Unmarshal([]byte(r), msg); err != nil {
				log.Errorf("json.Unmarshal(%s) error(%v)", r, err)
				continue
			}
			d.msgs <- msg
		}
		// ?????????????????????commit????????????commit???????????????commit offset????????????????????????
		time.Sleep(time.Millisecond * 10)
	}
}

// Messages get message chan.
func (d *Databus) Messages() <-chan *Message {
	return d.msgs
}

// Send send message to databus.
func (d *Databus) Send(c context.Context, k string, v interface{}) (err error) {
	var b []byte

	// send message
	if b, err = json.Marshal(v); err != nil {
		log.Errorf("json.Marshal(%v) error(%v)", v, err)
		return
	}
	if err = d.rdc.Do(context.TODO(), _cmdPub, k, b).Err(); err != nil {
		log.Errorf("conn.Do(%s,%s,%s) error(%v)", _cmdPub, k, b, err)
	}
	return
}

// Close close databus conn.
func (d *Databus) Close() (err error) {
	if !atomic.CompareAndSwapInt32(&d.closed, _open, _closed) {
		return
	}
	if d.rdc != nil {
		d.rdc.Close()
	}
	return nil
}
