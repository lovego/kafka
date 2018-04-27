package kafka

import (
	"encoding/json"
	"io"
	"time"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/lovego/logger"
)

type Consumer struct {
	KafkaAddrs []string
	Handler    func(*sarama.ConsumerMessage) (string, interface{})
	Client     *cluster.Client
	Consumer   *cluster.Consumer
	Producer   sarama.SyncProducer
	RespTopic  string
	Logfile    io.Writer
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
	at := time.Now()
	respTopicSuffix, resp := c.Handler(msg)
	respBytes, err := json.Marshal(resp)
	if resp != nil && err == nil {
		if respTopic := c.RespTopic + respTopicSuffix; respTopic != "" {
			c.Produce(respTopic, respBytes)
		}
	} else {
		c.Logger.Error(err)
	}
	c.Log(at, msg, respBytes)
}

func (c *Consumer) Produce(topic string, resp []byte) {
	if _, _, err := c.Producer.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(resp),
	}); err != nil {
		c.Logger.Error(err)
	}
}

func (c *Consumer) Log(at time.Time, msg *sarama.ConsumerMessage, resp []byte) {
	var log = struct {
		At       string          `json:"at"`
		Duration int64           `json:"duration"`
		Msg      json.RawMessage `json:"msg"`
		Resp     json.RawMessage `json:"resp"`
	}{
		At:       at.Format(time.RFC3339),
		Duration: int64(time.Since(at) / time.Millisecond),
		Msg:      json.RawMessage(msg.Value),
		Resp:     json.RawMessage(resp),
	}
	if buf, err := json.Marshal(log); err == nil {
		buf = append(buf, '\n')
		if _, err := c.Logfile.Write(buf); err != nil {
			c.Logger.Error(err)
		}
	} else {
		c.Logger.Error(err)
	}
}
