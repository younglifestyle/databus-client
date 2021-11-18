package databus_test

import (
	"context"
	"testing"
	"time"

	databus "databus-client"
)

var (
	pCfg = &databus.Config{
		// Key:          "0PvKGhAqDvsK7zitmS8t",
		// Secret:       "0PvKGhAqDvsK7zitmS8u",
		// Group:        "databus_test_group",
		// Topic:        "databus_test_topic",
		Key:    "dbe67e6a4c36f877",
		Secret: "8c775ea242caa367ba5c876c04576571",
		Group:  "Test1-MainCommonArch-P",
		Topic:  "test1",
		Action: "pub",
		Name:   "databus",
		Proto:  "tcp",
		// Addr:         "172.16.33.158:6205",
		Addr:         "127.0.0.1:6205",
		PoolSize:     10,
		Idle:         5,
		DialTimeout:  time.Duration(time.Second),
		WriteTimeout: time.Duration(time.Second),
		ReadTimeout:  time.Duration(time.Second),
		IdleTimeout:  time.Duration(time.Minute),
	}
	sCfg = &databus.Config{
		// Key:          "0PvKGhAqDvsK7zitmS8t",
		// Secret:       "0PvKGhAqDvsK7zitmS8u",
		// Group:        "databus_test_group",
		// Topic:        "databus_test_topic",
		Key:    "dbe67e6a4c36f877",
		Secret: "8c775ea242caa367ba5c876c04576571",
		Group:  "example",
		Topic:  "test1",
		Action: "sub",
		Name:   "databus",
		Proto:  "tcp",
		// Addr:         "172.16.33.158:6205",
		Addr:         "127.0.0.1:6205",
		PoolSize:     10,
		Idle:         5,
		DialTimeout:  time.Duration(time.Second),
		WriteTimeout: time.Duration(time.Second),
		ReadTimeout:  time.Duration(time.Second * 35),
		IdleTimeout:  time.Duration(time.Minute),
	}
)

type TestMsg struct {
	Now int64 `json:"now"`
}

func testSub(t *testing.T, d *databus.Databus) {
	for {
		m, ok := <-d.Messages()
		if !ok {
			return
		}
		t.Logf("sub message: %s", string(m.Value))
		if err := m.Commit(); err != nil {
			t.Errorf("sub commit error(%v)\n", err)
		}
	}
}

func testPub(t *testing.T, d *databus.Databus) {
	// pub
	m := &TestMsg{Now: time.Now().UnixNano()}
	if err := d.Send(context.TODO(), "test", m); err != nil {
		t.Errorf("d.Send(test) error(%v)", err)
	} else {
		t.Logf("pub message %v", m)
	}
}

func TestDatabus(t *testing.T) {
	d := databus.New(pCfg)
	// pub
	testPub(t, d)
	testPub(t, d)
	testPub(t, d)
	d.Close()

	// sub
	d = databus.New(sCfg)
	go testSub(t, d)

	time.Sleep(time.Second * 15)
	d.Close()
}

func TestDiscoveryDatabus(t *testing.T) {
	d := databus.New(pCfg)
	// pub
	testPub(t, d)
	testPub(t, d)
	testPub(t, d)
	d.Close()

	// sub
	d = databus.New(sCfg)
	go testSub(t, d)

	time.Sleep(time.Second * 15)
	d.Close()
}

func BenchmarkPub(b *testing.B) {
	d := databus.New(pCfg)
	defer d.Close()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m := &TestMsg{Now: time.Now().UnixNano()}
			if err := d.Send(context.TODO(), "test", m); err != nil {
				b.Errorf("d.Send(test) error(%v)", err)
				continue
			}
		}
	})
}

func BenchmarkDiscoveryPub(b *testing.B) {
	d := databus.New(pCfg)
	defer d.Close()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m := &TestMsg{Now: time.Now().UnixNano()}
			if err := d.Send(context.TODO(), "test", m); err != nil {
				b.Errorf("d.Send(test) error(%v)", err)
				continue
			}
		}
	})
}
