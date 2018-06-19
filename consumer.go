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

type Consumer struct {
	KafkaAddrs []string
	Handler    func(context.Context, *sarama.ConsumerMessage, int) (string, interface{}, bool, error)
	Client     *cluster.Client
	Consumer   *cluster.Consumer
	Producer   sarama.SyncProducer
	RespTopic  string
	Logger     *logger.Logger
}

func (c *Consumer) Consume(group string, topics []string, commit bool) {
	defer func() {
		if c.Producer != nil {
			c.Producer.Close()
		}
		if c.Consumer != nil {
			c.Consumer.Close()
		}
		if c.Client != nil {
			c.Client.Close()
		}
	}()

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
	} else {
		c.Client = client
	}

	consumer, err := cluster.NewConsumerFromClient(client, group, topics)
	if err != nil {
		c.Logger.Error(err)
		return false
	} else {
		c.Consumer = consumer
	}

	if producer, err := sarama.NewSyncProducerFromClient(client); err != nil {
		c.Logger.Error(err)
		return false
	} else {
		c.Producer = producer
	}

	return true
}

func (c *Consumer) Process(msg *sarama.ConsumerMessage) {
	debug := os.Getenv(`debug-kafka`) != ``

	var retryTimes = 0
	var respBytes []byte
	var endLoop bool
	var workFunc = func(ctx context.Context) error {
		respTopicSuffix, resp, retry, err := c.Handler(ctx, msg, retryTimes)
		if !retry {
			endLoop = true
		}
		if err != nil {
			return err
		}
		if resp != nil {
			respBytes, err = json.Marshal(resp)
			if err != nil {
				return err
			}
			if respTopic := c.RespTopic + respTopicSuffix; respTopic != "" {
				if err := c.Produce(respTopic, respBytes); err != nil {
					return err
				}
			}
		}
		return nil
	}
	var fieldsFunc = func(f *logger.Fields) {
		f.With(`msg`, json.RawMessage(msg.Value))
		f.With(`resp`, json.RawMessage(respBytes))
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

func (c *Consumer) Produce(topic string, resp []byte) error {
	if _, _, err := c.Producer.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(resp),
	}); err != nil {
		return err
	}
	return nil
}
