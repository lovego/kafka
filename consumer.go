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
	Producer   sarama.SyncProducer
	RespTopic  string
	Logger     *logger.Logger
	Logfile    io.Writer
}

func (c Consumer) Consume(topics []string, group string) {
	conf := cluster.NewConfig()
	conf.Consumer.Return.Errors = true
	// conf.Group.Return.Notifications = true
	conf.Consumer.Offsets.Initial = sarama.OffsetOldest
	consumer, err := cluster.NewConsumer(c.KafkaAddrs, group, topics, conf)
	if err != nil {
		c.Logger.Error(err)
		return
	}
	defer consumer.Close()

	defer func() {
		if c.Producer != nil {
			c.Producer.Close()
		}
	}()

	for {
		select {
		case msg := <-consumer.Messages():
			c.Process(msg)
			consumer.MarkOffset(msg, "")
		case err := <-consumer.Errors():
			c.Logger.Error(err)
		case ntf := <-consumer.Notifications():
			c.Logger.Print(ntf)
		}
	}
}

func (c Consumer) Process(msg *sarama.ConsumerMessage) {
	respTopicSuffix, resp := c.Handler(msg)
	respBytes, err := json.Marshal(resp)
	if err == nil {
		if respTopic := c.RespTopic + respTopicSuffix; respTopic != "" {
			c.Produce(respTopic, respBytes)
		}
	} else {
		c.Logger.Error(err)
	}
	c.Log(msg, respBytes)
}

func (c *Consumer) Produce(topic string, resp []byte) {
	if c.Producer == nil {
		producer, err := sarama.NewSyncProducer(c.KafkaAddrs, nil)
		if err != nil {
			c.Logger.Error(err)
			return
		}
		c.Producer = producer
	}

	if _, _, err := c.Producer.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(resp),
	}); err != nil {
		c.Logger.Error(err)
	}
}

func (c Consumer) Log(msg *sarama.ConsumerMessage, resp []byte) {
	var log = struct {
		At   string          `json:"at"`
		Msg  json.RawMessage `json:"msg"`
		Resp json.RawMessage `json:"resp"`
	}{
		At:   time.Now().Format(time.RFC3339),
		Msg:  json.RawMessage(msg.Value),
		Resp: json.RawMessage(resp),
	}
	if buf, err := json.Marshal(log); err == nil {
		if _, err := c.Logfile.Write(buf); err != nil {
			c.Logger.Error(err)
		}
	} else {
		c.Logger.Error(err)
	}
}
