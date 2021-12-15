package pubsub

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

//MatchesProducer qwe
type MatchesProducer struct {
	p *kafka.Producer
}

//InitMatchesProducer qwe
func InitMatchesProducer(cfg kafka.ConfigMap) (*MatchesProducer, error) {
	p, err := kafka.NewProducer(&cfg)

	if err != nil {
		return nil, err
	}

	return &MatchesProducer{p: p}, nil
}

//SendMessage Takes matchId and produces message
func (p MatchesProducer) SendMessage(matchID string) error {
	deliveryChan := make(chan kafka.Event)
	topic := "matches"
	p.p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value: []byte(matchID),
	}, deliveryChan)

	e := <-deliveryChan
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		fmt.Printf("Delivery failed: %v", m.TopicPartition.Error)
	}

	return nil
}
