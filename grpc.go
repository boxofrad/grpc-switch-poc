package main

import (
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/resolver"
)

type BalancerBuilder struct {
	name string

	mu       sync.Mutex
	byTarget map[string]*pickerBuilder
}

func NewBalancerBuilder(name string) *BalancerBuilder {
	return &BalancerBuilder{
		name:     name,
		byTarget: make(map[string]*pickerBuilder),
	}
}

func (b *BalancerBuilder) Name() string { return b.name }

var globalBalancerBuilder = NewBalancerBuilder("custom-balancer")

func init() {
	balancer.Register(globalBalancerBuilder)
}

func (b *BalancerBuilder) Build(cc balancer.ClientConn, opts balancer.BuildOptions) balancer.Balancer {
	targetURL := opts.Target.URL.String()

	b.mu.Lock()
	pb, ok := b.byTarget[targetURL]
	if !ok {
		pb = newPickerBuilder()
		b.byTarget[targetURL] = pb
	}
	b.mu.Unlock()

	return &Balancer{
		Balancer:      base.NewBalancerBuilder("", pb, base.Config{}).Build(cc, opts),
		pickerBuilder: pb,
	}
}

func (b *BalancerBuilder) Rebalance(dc string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for targetURL, pb := range b.byTarget {
		// TODO: implement this properly.
		if strings.HasSuffix(targetURL, dc) {
			pb.rebalance()
		}
	}
}

type Balancer struct {
	balancer.Balancer

	pickerBuilder *pickerBuilder
}

func (b *Balancer) UpdateClientConnState(state balancer.ClientConnState) error {
	err := b.Balancer.UpdateClientConnState(state)
	b.pickerBuilder.forgetOldConns(state.ResolverState.Addresses)
	return err
}

type connInfo struct {
	addr resolver.Address
	conn balancer.SubConn

	index    uint16
	failedAt uint64
	ready    bool
}

func newPickerBuilder() *pickerBuilder {
	b := &pickerBuilder{
		connInfo: resolver.NewAddressMap(),
		shuffler: rand.New(rand.NewSource(time.Now().UnixMicro())),
	}
	b.picker = &picker{builder: b}
	return b
}

type pickerBuilder struct {
	picker   *picker
	shuffler *rand.Rand

	pinned atomic.Pointer[pinnedConn]

	mu           sync.Mutex
	maxIndex     uint16
	failureClock uint64
	connInfo     *resolver.AddressMap
}

type pinnedConn struct {
	addr resolver.Address
	conn balancer.SubConn
}

func (pb *pickerBuilder) forgetOldConns(addrs []resolver.Address) {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	seen := resolver.NewAddressMap()
	for _, addr := range addrs {
		seen.Set(addr, struct{}{})
	}

	for _, addr := range pb.connInfo.Keys() {
		if _, ok := seen.Get(addr); !ok {
			pb.connInfo.Delete(addr)
		}
	}

	pb.updatePinnedServerLocked()
}

func (pb *pickerBuilder) Build(info base.PickerBuildInfo) balancer.Picker {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	ready := make(map[resolver.Address]struct{})

	// Track any new connections and mark any existing ones as ready (if they were
	// previously in a different state).
	for c, i := range info.ReadySCs {
		ready[i.Address] = struct{}{}

		if v, ok := pb.connInfo.Get(i.Address); ok {
			v.(*connInfo).ready = true
		} else {
			pb.connInfo.Set(i.Address, &connInfo{
				conn:  c,
				addr:  i.Address,
				index: pb.maxIndex,
				ready: true,
			})
			pb.maxIndex++
		}
	}

	// Anything not in the ReadySCs must've transitioned to another state (e.g.
	// transient faliure) so mark it as not ready.
	for _, addr := range pb.connInfo.Keys() {
		if _, isReady := ready[addr]; !isReady {
			v, _ := pb.connInfo.Get(addr)
			v.(*connInfo).ready = false
		}
	}

	pb.updatePinnedServerLocked()

	return pb.picker
}

func (pb *pickerBuilder) updatePinnedServerLocked() {
	candidates := make([]*connInfo, 0, pb.connInfo.Len())
	for _, v := range pb.connInfo.Values() {
		if info := v.(*connInfo); info.ready {
			candidates = append(candidates, info)
		}
	}

	if len(candidates) == 0 {
		pb.pinned.Store(nil)
		return
	}

	sort.Slice(candidates, func(a, b int) bool {
		ac, bc := candidates[a], candidates[b]
		return ac.failedAt < bc.failedAt ||
			(ac.failedAt == bc.failedAt && ac.index < bc.index)
	})

	winner := candidates[0]

	fmt.Println("Update pinned server", winner.addr.Addr)
	pb.pinned.Store(&pinnedConn{
		addr: winner.addr,
		conn: winner.conn,
	})
}

func (pb *pickerBuilder) rebalance() {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	conns := make([]*connInfo, pb.connInfo.Len())
	for idx, v := range pb.connInfo.Values() {
		conns[idx] = v.(*connInfo)
	}

	pb.shuffler.Shuffle(len(conns), func(a, b int) {
		conns[a], conns[b] = conns[b], conns[a]
	})

	for idx, conn := range conns {
		conn.index = uint16(idx)
	}

	pb.updatePinnedServerLocked()
}

func (pb *pickerBuilder) witnessError(addr resolver.Address, err error) {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	fmt.Println("Witness error", addr.Addr, err)

	v, ok := pb.connInfo.Get(addr)
	if !ok {
		return
	}

	pb.failureClock++
	v.(*connInfo).failedAt = pb.failureClock

	pb.updatePinnedServerLocked()
}

type picker struct {
	builder *pickerBuilder
}

func (p *picker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	pinned := p.builder.pinned.Load()

	if pinned == nil {
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}

	return balancer.PickResult{
		SubConn: pinned.conn,
		Done: func(info balancer.DoneInfo) {
			if info.Err != nil {
				p.builder.witnessError(pinned.addr, info.Err)
			}
		},
	}, nil
}
