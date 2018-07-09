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
//  Processor retry default true,
//  and you can return false with handler third returns if you do not want.
func newProcessor(c *Consumer, msg *sarama.ConsumerMessage) *Processor {
	return &Processor{c: c, msg: msg, retry: true}
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
	var v interface{}
	if err := json.Unmarshal([]byte(`"a"`), &v); err == nil {
		f.With("data", json.RawMessage(p.msg.Value))
	} else {
		f.With("dataRaw", string(p.msg.Value))
	}
	f.With("resp", json.RawMessage(p.respBytes))
}
