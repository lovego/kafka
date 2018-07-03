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
)
var (
	testDate = time.Now().Round(time.Second)
	testData = `test data`
)

type testContent struct {
	At   time.Time `json:"at"`
	Data string    `json:"data"`
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
	testConsumer.Close()

	err := Produce(testKafkaAddrs, testConsumerTopic, testContent{testDate, testData})
	if err != nil {
		t.Fatal(err)
	}
	msgs, err := Consume(testKafkaAddrs, []string{testProducerTopic}, `test-consumer`, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) == 0 {
		t.Fatal("does not get any message.")
	}
	data := testContent{}
	if err := json.Unmarshal(msgs[0].Value, &data); err != nil {
		t.Fatal(err)
	}
	if err := checkTestData(data); err != nil {
		t.Fatal(err)
	}
}

func testConsumerHandler(
	ctx context.Context, msg *sarama.ConsumerMessage, retryTimes int,
) (string, interface{}, bool, error) {
	var retry = true
	if retryTimes > 0 {
		retry = false
	}
	data := testContent{}
	err := json.Unmarshal(msg.Value, &data)
	if err != nil {
		return "", nil, retry, err
	}
	if err := checkTestData(data); err != nil {
		return "", nil, retry, err
	}
	return "", data, retry, nil
}

func checkTestData(data testContent) error {
	if !data.At.Equal(testDate) {
		return errors.New("consume: at not equal")
	}
	if data.Data != testData {
		return errors.New("consume: data not equal")
	}
	return nil
}
