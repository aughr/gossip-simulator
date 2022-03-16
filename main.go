package main

import (
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"sync"
	"time"

	_ "net/http/pprof"

	"github.com/HdrHistogram/hdrhistogram-go"
)

type config struct {
	members               int
	broadcastCount        int
	broadcastInterval     int
	heartbeatInterval     int
	broadcastMessageLimit int
	broadcastMult         int
	broadcastOldestFirst  bool
}

func main() {
	rand.Seed(time.Now().UnixNano())

	c := &config{}

	flag.IntVar(&c.members, "members", 100, "how many ring members to simulate")
	flag.IntVar(&c.broadcastCount, "broadcast-count", 4, "the number of members to broadcast to each interval")
	flag.IntVar(&c.broadcastInterval, "broadcast-interval", 200, "ms between broadcasts")
	flag.IntVar(&c.heartbeatInterval, "heartbeat-interval", 5000, "ms between heartbeats")
	flag.BoolVar(&c.broadcastOldestFirst, "broadcast-oldest-first", false, "whether to send the oldest updates first")
	flag.IntVar(&c.broadcastMessageLimit, "broadcast-message-limit", -1, "the number of messages allowed per broadcast. if zero or below, no limit")
	flag.IntVar(&c.broadcastMult, "broadcast-mult", -1, "sets the broadcast count akin to Hashicorp's memberlist RetransmitMult: mult * ceil(log10(members + 1)). if zero or below, inactive. this takes precedence over broadcast-count.")
	flag.Parse()

	if c.broadcastMult > 0 {
		c.broadcastCount = retransmitLimit(c.broadcastMult, c.members)
		log.Printf("Set broadcast-count to %d based on %d members and a mult of %d", c.broadcastCount, c.members, c.broadcastMult)
	}

	close := make(chan interface{})

	var r ring
	r.members = make([]chan []update, c.members)

	for index := 0; index < c.members; index++ {
		c := make(chan []update, len(r.members))
		r.members[index] = c
	}

	s := newStats(c.members)

	var wg sync.WaitGroup
	for index := 0; index < c.members; index++ {
		wg.Add(1)
		go member(r, c, index, r.members[index], close, s)
	}

	go s.run(5 * time.Second)

	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	wg.Wait()
}

// from Hashicorp's memberlist
func retransmitLimit(retransmitMult, n int) int {
	nodeScale := math.Ceil(math.Log10(float64(n + 1)))
	limit := retransmitMult * int(nodeScale)
	return limit
}

type update struct {
	index     int
	heartbeat time.Time
}

type updates []update

// Len is the number of elements in the collection.
func (u updates) Len() int {
	return len(u)
}

// Less reports whether the element with index i
// must sort before the element with index j.
//
// If both Less(i, j) and Less(j, i) are false,
// then the elements at index i and j are considered equal.
// Sort may place equal elements in any order in the final result,
// while Stable preserves the original input order of equal elements.
//
// Less must describe a transitive ordering:
//  - if both Less(i, j) and Less(j, k) are true, then Less(i, k) must be true as well.
//  - if both Less(i, j) and Less(j, k) are false, then Less(i, k) must be false as well.
//
// Note that floating-point comparison (the < operator on float32 or float64 values)
// is not a transitive ordering when not-a-number (NaN) values are involved.
// See Float64Slice.Less for a correct implementation for floating-point values.
func (u updates) Less(i int, j int) bool {
	return u[i].heartbeat.Before(u[j].heartbeat)
}

// Swap swaps the elements with indexes i and j.
func (u updates) Swap(i int, j int) {
	temp := u[i]
	u[i] = u[j]
	u[j] = temp
}

type ring struct {
	members []chan []update
}

func member(r ring, c *config, index int, input <-chan []update, close <-chan (interface{}), s *stats) {
	heartbeat := time.Now()

	state := make([]time.Time, len(r.members))
	state[index] = heartbeat

	var pending []update

	bci := time.Duration(c.broadcastInterval) * time.Millisecond
	bct := time.NewTicker(bci)
	hbt := time.NewTicker(time.Duration(c.heartbeatInterval) * time.Millisecond)

	received := 0

	sleep := time.Duration(rand.Intn(int(bci)))
	//log.Printf("Sleeping %v", sleep)
	time.Sleep(sleep)

	for {
		select {
		case <-hbt.C:
			heartbeat = time.Now()
			state[index] = heartbeat

			pending = append(pending, update{index, heartbeat})
		case <-bct.C:
			if c.broadcastOldestFirst {
				sort.Sort(updates(pending))
			}

			toSend := c.broadcastMessageLimit
			if toSend > len(pending) || c.broadcastMessageLimit < 0 {
				toSend = len(pending)
			}

			var batch []update
			if c.broadcastOldestFirst {
				batch = pending[:toSend]
				pending = pending[toSend:]
			} else {
				cut := len(pending) - toSend
				batch = pending[cut:]
				pending = pending[:cut]
			}

			for bc := 0; bc < c.broadcastCount; bc++ {
				target := rand.Intn(len(r.members))
				r.members[target] <- batch
			}

			s.sentInput <- len(batch)
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

			orig := pending
			pending = nil
			pending = append(pending, orig...)
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
		received:      hdrhistogram.NewWindowed(2, 0, int64(members)*10, 3),
		sent:          hdrhistogram.NewWindowed(2, 0, int64(members)*10, 3),
		delay:         hdrhistogram.NewWindowed(2, 0, 60*(int64(time.Second)/int64(time.Millisecond)), 3),
		receivedInput: make(chan int, members*5),
		sentInput:     make(chan int, members*5),
		delayInput:    make(chan int64, members*5),
	}
}

func (s *stats) dump() {
	fmt.Println("RECEIVED: How many updates did we receive between each time we sent a broacast?")
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
			err := s.delay.Current.RecordValue(d)
			if err != nil {
				_ = s.delay.Current.RecordValue(s.delay.Current.HighestTrackableValue())
			}
		}
	}
}
