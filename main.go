package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"sync"
	"time"

	_ "net/http/pprof"

	"github.com/HdrHistogram/hdrhistogram-go"
)

func main() {
	var members = flag.Int("members", 100, "how many ring members to simulate")
	flag.Parse()

	broadcastInterval := 200 * time.Millisecond
	heartbeatInterval := 5 * time.Second

	close := make(chan interface{})

	var r ring
	r.members = make([]chan []update, *members)

	for index := 0; index < *members; index++ {
		c := make(chan []update, len(r.members))
		r.members[index] = c
	}

	s := newStats(*members)

	var wg sync.WaitGroup
	for index := 0; index < *members; index++ {
		wg.Add(1)
		go member(r, index, broadcastInterval, heartbeatInterval, r.members[index], close, s)
	}

	go s.run(5 * time.Second)

	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	wg.Wait()
}

type update struct {
	index     int
	heartbeat time.Time
}

type ring struct {
	members []chan []update
}

func member(r ring, index int, broadcastInterval, heartbeatInterval time.Duration, input <-chan []update, close <-chan (interface{}), s *stats) {
	heartbeat := time.Now()

	state := make([]time.Time, len(r.members))
	state[index] = heartbeat

	var pending []update

	bct := time.NewTicker(broadcastInterval)
	hbt := time.NewTicker(heartbeatInterval)

	received := 0

	time.Sleep(time.Duration(rand.Intn(int(heartbeatInterval))))

	for {
		select {
		case <-hbt.C:
			heartbeat = time.Now()
			state[index] = heartbeat

			pending = append(pending, update{index, heartbeat})
		case <-bct.C:
			for bc := 0; bc < 6; bc++ {
				target := rand.Intn(len(r.members))
				r.members[target] <- pending
			}

			if float64(len(pending)) > float64(len(r.members))*0.5 {
				//log.Printf("%d: sent %d messages which is >0.5 of the number of members", index, len(pending))
			}

			s.sentInput <- len(pending)
			s.receivedInput <- received

			var maxDelay time.Duration
			now := time.Now()
			for _, hb := range state {
				diff := now.Sub(hb)
				if diff > maxDelay {
					maxDelay = diff
				}
			}
			s.delayInput <- int64(maxDelay / time.Millisecond)

			pending = nil
			received = 0
		case messages := <-input:
			received += len(messages)

			for _, m := range messages {
				if m.heartbeat.After(state[m.index]) {
					state[m.index] = m.heartbeat
					pending = append(pending, m)
				}
			}
		case <-close:
			return
		}
	}
}

type stats struct {
	received *hdrhistogram.WindowedHistogram
	sent     *hdrhistogram.WindowedHistogram
	delay    *hdrhistogram.WindowedHistogram

	receivedInput chan int
	sentInput     chan int
	delayInput    chan int64
}

func newStats(members int) *stats {
	return &stats{
		received:      hdrhistogram.NewWindowed(5, 0, int64(members)*10, 3),
		sent:          hdrhistogram.NewWindowed(5, 0, int64(members)*10, 3),
		delay:         hdrhistogram.NewWindowed(5, 0, 20*(int64(time.Second)/int64(time.Millisecond)), 3),
		receivedInput: make(chan int, members*5),
		sentInput:     make(chan int, members*5),
		delayInput:    make(chan int64, members*5),
	}
}

func (s *stats) dump() {
	fmt.Println("RECEIVED: How many updates did we recieve between each time we sent a broacast?")
	s.received.Merge().PercentilesPrint(os.Stdout, 1, 1.0)

	fmt.Println("\n\nSENT: How many updates did we send in each broadcast?")
	s.sent.Merge().PercentilesPrint(os.Stdout, 1, 1.0)
	fmt.Print("\n\n\n")

	fmt.Println("\n\nDELAY: How out of date is the worst heartbeat?")
	s.delay.Merge().PercentilesPrint(os.Stdout, 1, 1.0)
	fmt.Print("\n\n\n")
}

func (s *stats) run(interval time.Duration) {
	ticker := time.NewTicker(interval)

	for {
		select {
		case <-ticker.C:
			s.dump()

			s.received.Rotate()
			s.sent.Rotate()
			s.delay.Rotate()
		case c := <-s.sentInput:
			_ = s.sent.Current.RecordValue(int64(c))
		case c := <-s.receivedInput:
			_ = s.received.Current.RecordValue(int64(c))
		case d := <-s.delayInput:
			_ = s.delay.Current.RecordValue(d)
		}
	}
}
