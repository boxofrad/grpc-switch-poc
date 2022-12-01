package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/tap"
)

var (
	serverMode bool
	addrs      string
)

func main() {
	flag.BoolVar(&serverMode, "server", false, "")
	flag.StringVar(&addrs, "addrs", "", "")
	flag.Parse()

	if serverMode {
		if err := runServer(); err != nil {
			log.Fatal(err)
		}
	} else {
		if err := runClient(addrs); err != nil {
			log.Fatal(err)
		}
	}
}

func runServer() error {
	server := grpc.NewServer(
		grpc.InTapHandle(func(ctx context.Context, info *tap.Info) (context.Context, error) {
			return nil, status.Error(codes.ResourceExhausted, "ruh roh")
		}),
	)
	healthpb.RegisterHealthServer(server, health.NewServer())

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}

	fmt.Printf("Listening on %s\n", lis.Addr())
	return server.Serve(lis)
}

func runClient(addrsStr string) error {
	if addrsStr == "" {
		return errors.New("-addrs is required")
	}
	addrs := strings.Split(addrsStr, ",")
	resolver := registerResolver(addrs)

	conn, err := grpc.Dial(resolverScheme+"://server.dc1",
		grpc.WithInsecure(),
		grpc.WithBlock(),
		grpc.WithDefaultServiceConfig(
			fmt.Sprintf(`{"loadBalancingPolicy":"%s"}`, globalBalancerBuilder.Name()),
		),
		grpc.WithUnaryInterceptor(RetryInterceptor()),
	)
	if err != nil {
		return err
	}

	for i := 0; i < 10; i++ {
		fmt.Println("Rebalance!")
		globalBalancerBuilder.Rebalance("dc1")
	}

	client := healthpb.NewHealthClient(conn)

	fmt.Println("===> RPC 1")
	_, _ = client.Check(context.Background(), &healthpb.HealthCheckRequest{})

	fmt.Println("===> RPC 2")
	_, _ = client.Check(context.Background(), &healthpb.HealthCheckRequest{})

	fmt.Println("===> Remove addr", addrs[1])
	resolver.setAddrs(addrs[:1])

	return nil
}
