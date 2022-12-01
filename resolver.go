package main

import (
	"sync"

	"google.golang.org/grpc/resolver"
)

const resolverScheme = "custom-resolver"

type resolverBuilder struct {
	mu    sync.Mutex
	conns []resolver.ClientConn
	addrs []resolver.Address
}

func registerResolver(addrs []string) *resolverBuilder {
	b := &resolverBuilder{}
	b.setAddrs(addrs)

	resolver.Register(b)
	return b
}

func (rb *resolverBuilder) setAddrs(addrs []string) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	grpcAddrs := make([]resolver.Address, len(addrs))
	for idx, a := range addrs {
		grpcAddrs[idx] = resolver.Address{Addr: a}
	}
	rb.addrs = grpcAddrs

	for _, cc := range rb.conns {
		cc.UpdateState(resolver.State{Addresses: grpcAddrs})
	}
}

func (rb *resolverBuilder) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	rb.conns = append(rb.conns, cc)

	cc.UpdateState(resolver.State{Addresses: rb.addrs})

	return staticResolver{}, nil
}

func (*resolverBuilder) Scheme() string { return resolverScheme }

type staticResolver struct{}

func (staticResolver) Close()                                {}
func (staticResolver) ResolveNow(resolver.ResolveNowOptions) {}
