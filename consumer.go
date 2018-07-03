package kafka

import (
	"context"
	"os"
	"time"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/lovego/logger"
)

var debug = os.Getenv("debug-kafka") != ""

// kafka consumer struct
type Consumer struct {
	KafkaAddrs   []string
	KafkaVersion sarama.KafkaVersion
	Handler      func(context.Context, *sarama.ConsumerMessage, int) (string, interface{}, bool, error)
	Client       *cluster.Client
	Consumer     *cluster.Consumer
	Producer     sarama.SyncProducer
	RespTopic    string
	Logger       *logger.Logger
}

// start consume, and it can produce response
func (c *Consumer) Consume(group string, topics []string, commit bool) {
	defer c.Close()

	if !c.setup(group, topics) {
		return
	}

	var messagesChannel = c.Consumer.Messages()
	var errorsChannel = c.Consumer.Errors()
	for {
		select {
		case msg := <-messagesChannel:
			c.Process(msg)
			c.Consumer.MarkOffset(msg, "")
			if commit {
				if err := c.Consumer.CommitOffsets(); err != nil {
					c.Logger.Error(err)
				}
			}
		case err := <-errorsChannel:
			c.Logger.Error(err)
			/*
				case ntf := <-consumer.Notifications():
					c.Logger.Print(ntf)
			*/
		}
	}
}

func (c *Consumer) setup(group string, topics []string) bool {
	conf := cluster.NewConfig()
	conf.Producer.Return.Successes = true
	conf.Consumer.Return.Errors = true
	conf.Consumer.Offsets.Initial = sarama.OffsetOldest
	conf.Version = c.KafkaVersion
	// conf.Consumer.Offsets.CommitInterval = time.Second
	// conf.Group.Return.Notifications = true
	client, err := cluster.NewClient(c.KafkaAddrs, conf)
	if err != nil {
		c.Logger.Error(err)
		return false
	}
	c.Client = client

	consumer, err := cluster.NewConsumerFromClient(client, group, topics)
	if err != nil {
		c.Logger.Error(err)
		return false
	}
	c.Consumer = consumer

	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		c.Logger.Error(err)
		return false
	}
	c.Producer = producer

	return true
}

// process data after consume
func (c *Consumer) Process(msg *sarama.ConsumerMessage) {
	p := newProcessor(c, msg)

	var wait = time.Second
	for p.retry {
		c.Logger.Record(debug, p.WorkFunc, nil, p.FieldsFunc)
		p.retryTimes++
		if wait < time.Minute {
			wait += wait
			if wait > time.Minute {
				wait = time.Minute
			}
		}
		time.Sleep(wait)
	}
}

// produce the response if necessary
func (c *Consumer) Produce(topic string, resp []byte) error {
	if _, _, err := c.Producer.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(resp),
	}); err != nil {
		return err
	}
	return nil
}

// close Consumer
func (c *Consumer) Close() {
	if c.Producer != nil {
		c.Producer.Close()
	}
	if c.Consumer != nil {
		c.Consumer.Close()
	}
	if c.Client != nil {
		c.Client.Close()
	}
}
