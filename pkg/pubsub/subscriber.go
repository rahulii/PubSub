package pubsub

import (
	"context"

	"github.com/gofrs/uuid"
	pb "github.com/rahulii/pubsub/grpc/pb/pub-sub"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Subscriber struct {
	brokerAddress string
	grpcClient    pb.PubSubServiceClient
	cc            *grpc.ClientConn
	ctx           context.Context
	cancel        context.CancelFunc
	ID            uuid.UUID
	// subscriptions is a map of topic to the subscriber's stream.
	subscriptions map[string]pb.PubSubService_SubscribeClient
	Messages      chan *pb.SubscribeResponse
}

// NewSubscriber creates a new subscriber.
func NewSubscriber(brokerAddress string) (*Subscriber, error) {
	ctx, cancel := context.WithCancel(context.Background())

	conn, err := grpc.Dial(
		brokerAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		cancel()
		return nil, err
	}

	return &Subscriber{
		brokerAddress: brokerAddress,
		grpcClient:    pb.NewPubSubServiceClient(conn),
		cc:            conn,
		ctx:           ctx,
		cancel:        cancel,
		ID:            uuid.Must(uuid.NewV4()),
		subscriptions: make(map[string]pb.PubSubService_SubscribeClient),
		Messages:      make(chan *pb.SubscribeResponse),
	}, nil
}

// Subscribe subscribes to the topic.
func (s *Subscriber) Subscribe(topic string) error {
	// Check if the subscriber is already subscribed to the topic.
	if _, ok := s.subscriptions[topic]; ok {
		return nil
	}

	stream, err := s.grpcClient.Subscribe(s.ctx, &pb.SubscribeInfo{
		Topic:        topic,
		SubscriberId: &pb.UUID{Value: s.ID.String()},
	})
	if err != nil {
		return err
	}

	s.subscriptions[topic] = stream

	go s.recv(stream)

	return nil
}

func (s *Subscriber) recv(stream pb.PubSubService_SubscribeClient) {
	for {
		select {
		case <-s.ctx.Done():
			stream.CloseSend()
			return
		default:
			resp, err := stream.Recv()
			if err != nil {
				return
			}
			s.Messages <- resp
		}
	}
}

// Close closes the subscriber connection.
func (s *Subscriber) Close() error {
	s.cancel()
	return s.cc.Close()
}
