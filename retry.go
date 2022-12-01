package main

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func RetryInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		for attempts := 1; ; attempts++ {
			err := invoker(ctx, method, req, reply, cc, opts...)

			// TODO: lol, implement this properly.
			if status.Code(err) == codes.ResourceExhausted && attempts < 5 {
				time.Sleep(1 * time.Second)
				fmt.Println("Retrying!")
			} else {
				return err
			}
		}
	}
}
