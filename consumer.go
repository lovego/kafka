package kafka

import (
	"context"
	"encoding/json"
	"os"
	"time"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/lovego/logger"
)

var debug = os.Getenv("debug-kafka") != ""

// kafka consumer struct
type Consumer struct {
	KafkaAddrs []string
	Handler    func(context.Context, *sarama.ConsumerMessage, int) (string, interface{}, bool, error)
	Client     *cluster.Client
	Consumer   *cluster.Consumer
	Producer   sarama.SyncProducer
	RespTopic  string
	Logger     *logger.Logger
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
	// conf.Consumer.Offsets.CommitInterval = time.Second
	// conf.Group.Return.Notifications = true
	var client, err = cluster.NewClient(c.KafkaAddrs, conf)
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

	if producer, err := sarama.NewSyncProducerFromClient(client); err != nil {
		c.Logger.Error(err)
		return false
	}
	c.Producer = producer

	return true
}

// process data after consume
func (c *Consumer) Process(msg *sarama.ConsumerMessage) {
	var retryTimes = 0
	var respBytes []byte
	var endLoop bool
	var workFunc = func(ctx context.Context) error {
		respTopicSuffix, resp, retry, err := c.Handler(ctx, msg, retryTimes)
		respTopic := c.RespTopic + respTopicSuffix
		endLoop = !retry
		if respTopic == "" || resp == nil || err != nil {
			return err
		}
		if respBytes, err = json.Marshal(resp); err != nil {
			return err
		}
		return c.Produce(respTopic, respBytes)
	}
	var fieldsFunc = func(f *logger.Fields) {
		f.With("msg", json.RawMessage(msg.Value))
		f.With("resp", json.RawMessage(respBytes))
	}

	var wait = time.Second
	for {
		c.Logger.Record(debug, workFunc, nil, fieldsFunc)
		if endLoop {
			break
		}
		retryTimes++
		if wait < time.Minute {
			wait += wait
			if wait > time.Minute {
				wait = time.Minute
			}
		}
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
