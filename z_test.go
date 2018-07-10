package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/lovego/logger"
)

var (
	testKafkaAddrs    = []string{`localhost:9092`}
	testConsumerTopic = `consumer-test-topic`
	testProducerTopic = `producer-test-topic`
	testData          = testContent{time.Now().Round(time.Second), `test data`}
)

type testContent struct {
	At   time.Time `json:"at"`
	Data string    `json:"data"`
}

func testConsumerHandler(
	ctx context.Context, msg *sarama.ConsumerMessage, retryTimes int,
) (Result, error) {
	data := testContent{}
	if err := json.Unmarshal(msg.Value, &data); err != nil || retryTimes == 0 {
		return Result{NotJson: true, Retry: true}, nil
	}
	if err := checkTestData(data); err != nil {
		return Result{}, err
	}
	return Result{RespContent: data}, nil
}

func TestProduceAndConsume(t *testing.T) {
	var testConsumer = &Consumer{
		KafkaAddrs:   testKafkaAddrs,
		KafkaVersion: sarama.V0_10_1_0,
		Handler:      testConsumerHandler,
		RespTopic:    testProducerTopic,
		Logger:       logger.New(os.Stderr),
	}
	go testConsumer.Consume(`test-consumer`, []string{testConsumerTopic}, true)

	err := Produce(testKafkaAddrs, testConsumerTopic, testData)
	if err != nil {
		t.Fatal(err)
	}
	msgs, err := Consume(testKafkaAddrs, []string{testProducerTopic}, `test-consumer`, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 {
		t.Fatalf("unexpected messages count: %d.", len(msgs))
	}
	data := testContent{}
	if err := json.Unmarshal(msgs[0].Value, &data); err != nil {
		t.Fatal(err)
	}
	if err := checkTestData(data); err != nil {
		t.Fatal(err)
	}
}

func checkTestData(data testContent) error {
	if !data.At.Equal(testData.At) {
		return errors.New("consume: at not equal")
	}
	if data.Data != testData.Data {
		return errors.New("consume: data not equal")
	}
	return nil
}
