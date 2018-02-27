package kafka

import (
	"encoding/json"
	"time"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
)

func Produce(kafkaAddrs []string, topic string, content interface{}) error {
	producer, err := sarama.NewSyncProducer(kafkaAddrs, nil)
	if err != nil {
		return err
	}
	defer producer.Close()

	buf, err := json.Marshal(content)
	if err != nil {
		return err
	}
	msg := &sarama.ProducerMessage{
		Topic: topic, Value: sarama.ByteEncoder(buf),
	}
	_, _, err = producer.SendMessage(msg)
	return err
}

func Consume(kafkaAddrs, topics []string, group string, wait time.Duration) (
	msgs []*sarama.ConsumerMessage, err error,
) {
	conf := cluster.NewConfig()
	conf.Consumer.Return.Errors = true
	conf.Consumer.Offsets.Initial = sarama.OffsetOldest
	consumer, err := cluster.NewConsumer(kafkaAddrs, group, topics, conf)
	if err != nil {
		return
	}
	defer consumer.Close()

	for {
		select {
		case msg := <-consumer.Messages():
			msgs = append(msgs, msg)
			consumer.MarkOffset(msg, "")
		case err = <-consumer.Errors():
			return
		case <-time.After(wait): // default CommitInterval is 1s.
			return
		}
	}
	return
}
