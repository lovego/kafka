package kafka

import (
	"context"
	"encoding/json"

	"github.com/Shopify/sarama"
	"github.com/lovego/logger"
)

// kafka processor struct
type Processor struct {
	retryTimes int
	respBytes  []byte
	retry      bool
	c          *Consumer
	msg        *sarama.ConsumerMessage
}

// new a processor
func newProcessor(c *Consumer, msg *sarama.ConsumerMessage) *Processor {
	return &Processor{c: c, msg: msg}
}

// consumer process work func
func (p *Processor) WorkFunc(ctx context.Context) error {
	respTopicSuffix, resp, retry, err := p.c.Handler(ctx, p.msg, p.retryTimes)
	respTopic := p.c.RespTopic + respTopicSuffix
	p.retry = retry
	if respTopic == "" || resp == nil || err != nil {
		return err
	}
	if p.respBytes, err = json.Marshal(resp); err != nil {
		return err
	}
	return p.c.Produce(respTopic, p.respBytes)
}

// consumer process record fields func
func (p *Processor) FieldsFunc(f *logger.Fields) {
	f.With("_retry", p.retryTimes)
	f.With("data", json.RawMessage(p.msg.Value))
	f.With("resp", json.RawMessage(p.respBytes))
}
