package pubsub

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

//PlayersProducer qwe
type PlayersProducer struct {
	p *kafka.Producer
}

//InitPlayerProducer qwe
func InitPlayerProducer(cfg kafka.ConfigMap) (*PlayersProducer, error) {
	p, err := kafka.NewProducer(&cfg)

	if err != nil {
		return nil, err
	}

	return &PlayersProducer{p: p}, nil
}

//SendMessage Takes matchId and produces message
func (p PlayersProducer) SendMessage(puuid string) error {
	deliveryChan := make(chan kafka.Event)
	topic := "players"
	p.p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value: []byte(puuid),
	}, deliveryChan)

	e := <-deliveryChan
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		fmt.Printf("Delivery failed: %v", m.TopicPartition.Error)
	}

	return nil
}
