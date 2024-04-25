package tests

import (
	"testing"
	"time"

	"github.com/rahulii/pubsub/pkg/pubsub"
	"github.com/stretchr/testify/assert"
)

var broker pubsub.Broker

func setupBroker() {
	// Start the broker.
	broker = pubsub.NewBroker("localhost:50051")
	go func() {
		if err := broker.Start(); err != nil {
			panic(err)
		}
	}()
}

func teardownBroker() {
	// Stop the broker.
	if err := broker.Stop(); err != nil {
		panic(err)
	}
}

func Test_PubSub(t *testing.T) {
	setupBroker()

	t.Run("TestPubSub", func(t *testing.T) {
		t.Run("TestOnePublisherOneSubscriber", func(t *testing.T) {
			pub := NewPublisher(t, "localhost:50051")
			defer pub.Close()

			topic := "test"
			message := "hello"

			// subscribe to the topic.
			sub := NewSubscriber(t, "localhost:50051")
			defer sub.Close()

			err := sub.Subscribe(topic)

			assert.NoError(t, err)

			time.Sleep(1 * time.Second)

			// Publish a message to the topic.
			err = pub.Publish(topic, message)
			assert.NoError(t, err)

			// Receive the message.
			msg := <-sub.Messages
			assert.Equal(t, message, msg.Message)
		})
		t.Run("TestOnePublisherMultipleSubscribers", func(t *testing.T) {
			pub := NewPublisher(t, "localhost:50051")
			defer pub.Close()

			topic := "test-topic"
			message := "hello"

			// subscribe to the topic.
			sub1 := NewSubscriber(t, "localhost:50051")
			defer sub1.Close()

			err := sub1.Subscribe(topic)
			assert.NoError(t, err)

			sub2 := NewSubscriber(t, "localhost:50051")
			defer sub2.Close()

			err = sub2.Subscribe(topic)
			assert.NoError(t, err)

			time.Sleep(1 * time.Second)

			// Publish a message to the topic.
			if err := pub.Publish(topic, message); err != nil {
				t.Fatalf("failed to publish message: %v", err)
			}

			// Receive the message.
			msg1 := <-sub1.Messages
			assert.Equal(t, message, msg1.Message)

			msg2 := <-sub2.Messages
			assert.Equal(t, message, msg2.Message)
		})
	})

	teardownBroker()
}

func NewPublisher(t *testing.T, brokerAddress string) pubsub.Publisher {
	p, err := pubsub.NewPublisher(brokerAddress)
	assert.NoErrorf(t, err, "failed to create publisher: %v", err)
	return p
}

func NewSubscriber(t *testing.T, brokerAddress string) *pubsub.Subscriber {
	s, err := pubsub.NewSubscriber(brokerAddress)
	assert.NoErrorf(t, err, "failed to create subscriber: %v", err)
	return s
}
