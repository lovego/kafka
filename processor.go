package kafka

import (
	"context"
	"encoding/json"
	"time"

	"github.com/Shopify/sarama"
	"github.com/lovego/logger"
)

type Result struct {
	RespTopic      string
	RespContent    interface{}
	Retry, NotJson bool
	respBytes      []byte
}

// kafka message processor
type Processor struct {
	c   *Consumer
	msg *sarama.ConsumerMessage
	Result
	retryTimes int // 当前是第几次重试
	wait       time.Duration
}

// process kafka message
func Process(c *Consumer, msg *sarama.ConsumerMessage) {
	p := &Processor{c: c, msg: msg}

	for {
		c.Logger.Record(p.WorkFunc, nil, p.FieldsFunc)
		if !p.Result.Retry {
			return
		}
		p.waitForRetry()
	}
}

// consumer process work func
func (p *Processor) WorkFunc(ctx context.Context) error {
	result, err := p.c.Handler(ctx, p.msg, p.retryTimes)
	p.Result = result

	respTopic := p.c.RespTopic + result.RespTopic
	if respTopic == "" || result.RespContent == nil || err != nil {
		return err
	}
	if p.respBytes, err = json.Marshal(result.RespContent); err != nil {
		return err
	}
	return p.c.Produce(respTopic, p.respBytes)
}

// consumer process record fields func
func (p *Processor) FieldsFunc(f *logger.Fields) {
	f.With("_retry", p.retryTimes)
	if p.NotJson {
		f.With("dataRaw", string(p.msg.Value))
	} else {
		f.With("data", json.RawMessage(p.msg.Value))
	}
	f.With("resp", json.RawMessage(p.respBytes))
}

func (p *Processor) waitForRetry() {
	p.retryTimes++
	if p.wait == 0 {
		p.wait = time.Second
	} else if p.wait < time.Minute {
		p.wait += p.wait
		if p.wait > time.Minute {
			p.wait = time.Minute
		}
	}
	time.Sleep(p.wait)
}
