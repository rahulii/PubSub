package pubsub

import (
	"context"
	"net"
	"sync"

	pb "github.com/rahulii/pubsub/grpc/pb/pub-sub"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type Broker interface {
	Start() error
	Stop() error
}

// streamKey is used to uniquely identify a stream.
type streamKey struct {
	topic        string
	subscriberID *pb.UUID
}

// broker is a gRPC server that implements the Broker interface.
type broker struct {
	pb.UnimplementedPubSubServiceServer
	// subscribers is a map of topics to a map of subscriber IDs to the subscriber's stream.
	// This is used to keep track of all the subscribers to a topic.
	subscribers map[string]map[*pb.UUID]pb.PubSubService_SubscribeServer
	// streamLock is used to lock a stream when it is being written to.
	// This is used to prevent multiple goroutines from writing to the same stream at the same time.
	streamLock map[streamKey]*sync.Mutex
	mu         sync.RWMutex
	ctx        context.Context
	cancel     context.CancelFunc
	log        *zap.Logger
	address    string
	listener   net.Listener
	server     *grpc.Server
}

// NewBroker creates a new broker.
func NewBroker(address string) Broker {
	ctx, cancel := context.WithCancel(context.Background())
	return &broker{
		subscribers: make(map[string]map[*pb.UUID]pb.PubSubService_SubscribeServer),
		streamLock:  make(map[streamKey]*sync.Mutex),
		ctx:         ctx,
		cancel:      cancel,
		address:     address,
		log:         zap.Must(zap.NewProduction()),
		mu:          sync.RWMutex{},
	}
}

// Start starts the broker gRPC server.
func (b *broker) Start() error {
	if err := b.startGRPCServer(); err != nil {
		return err
	}

	return b.gracefulStop()
}

// Stop stops the broker.
func (b *broker) Stop() error {
	b.cancel()
	b.server.GracefulStop()
	return nil
}

func (b *broker) Publish(ctx context.Context, info *pb.PublishInfo) (*pb.PublishResponse, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	for subscriberID, stream := range b.subscribers[info.Topic] {
		// Lock the stream to prevent multiple goroutines from writing to the same stream at the same time.
		key := streamKey{
			topic:        info.GetTopic(),
			subscriberID: subscriberID,
		}
		b.streamLock[key].Lock()

		err := stream.Send(&pb.SubscribeResponse{
			Message: info.GetMessage(),
			Topic:   info.GetTopic(),
		})
		if err != nil {
			b.log.Error("error sending message to subscriber", zap.Error(err))
		}
		// Unlock the stream after writing to it.
		b.streamLock[key].Unlock()

		b.log.Info("message sent to subscriber", zap.String("topic", info.Topic), zap.String("subscriber_id", subscriberID.String()))
	}

	return &pb.PublishResponse{Success: true}, nil
}

// Subscribe is a streaming RPC that allows a client to subscribe to a topic.
func (b *broker) Subscribe(info *pb.SubscribeInfo, stream pb.PubSubService_SubscribeServer) error {
	b.mu.Lock()

	if _, ok := b.subscribers[info.Topic]; !ok {
		b.subscribers[info.Topic] = make(map[*pb.UUID]pb.PubSubService_SubscribeServer)
	}

	key := streamKey{topic: info.Topic, subscriberID: info.SubscriberId}
	b.subscribers[info.Topic][info.SubscriberId] = stream

	b.streamLock[key] = &sync.Mutex{}

	b.log.Info("new subscriber added", zap.String("topic", info.Topic), zap.String("subscriber_id", info.SubscriberId.String()))

	b.mu.Unlock()

	for {
		select {
		// wait for the context to be done.
		case <-b.ctx.Done():
			return nil
			// wait for the client to close the stream.
		case <-stream.Context().Done():
			return nil
		}
	}
}
