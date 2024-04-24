package pubsub

import (
	"net"
	"os"
	"os/signal"
	"syscall"

	pb "github.com/rahulii/pubsub/grpc/pb/pub-sub"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)


func (b *broker) startGRPCServer() error {
	var err error
	b.listener, err = net.Listen("tcp", b.address)
	if err != nil {
		return err
	}

	s := grpc.NewServer()
	pb.RegisterPubSubServiceServer(s, b)

	b.server = s

	b.log.Info("starting gRPC server", zap.String("address", b.address))

	go func() {
		if err := b.server.Serve(b.listener); err != nil {
			b.log.Error("gRPC server failed", zap.Error(err))
		}
	}()

	return nil
}

func (b *broker) gracefulStop() error{
	stopCh := make(chan os.Signal, 1)
	signal.Notify(stopCh, os.Interrupt, syscall.SIGTERM)

	<-stopCh
	return b.Stop()
}
