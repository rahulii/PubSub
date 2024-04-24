package pubsub

import (
	"context"

	pb "github.com/rahulii/pubsub/grpc/pb/pub-sub"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Publisher interface {
	// Publish opens a connection to the broker and publishes the message to the topic.
	Publish(topic string, message string) error
	// Close closes the publisher connection.
	Close() error
}

type publisher struct {
	brokerAddress string
	grpcClient    pb.PubSubServiceClient
	cc 		  *grpc.ClientConn
	ctx 	  context.Context
	cancel    context.CancelFunc
}

// NewPublisher creates a new publisher.
func NewPublisher(brokerAddress string) (Publisher, error) {
	ctx, cancel := context.WithCancel(context.Background())
	
	conn , err := grpc.Dial(
		brokerAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		cancel()
		return nil, err
	}

	return &publisher{
		brokerAddress: brokerAddress,
		grpcClient:    pb.NewPubSubServiceClient(conn),
		cc: 			conn,
		ctx: 			ctx,
		cancel: 		cancel,
	}, nil
}

// Publish publishes the message to the topic.
func (p *publisher) Publish(topic string, message string) error {
	_, err := p.grpcClient.Publish(p.ctx, &pb.PublishInfo{
		Topic:   topic,
		Message: message,
	})
	return err
}

// Close closes the publisher connection.
func (p *publisher) Close() error {
	p.cancel()
	return p.cc.Close()
}