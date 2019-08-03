package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"

	"github.com/Shopify/sarama"
)

const (
	// TopicName - Topics
	TopicName = "data-message-test"

	// GroupID - ConsumerGroup
	GroupID = "Your-Consumer-Group1"
)

var (
	procType = flag.String("procType", "", "Operation mode switching(Producer or Consumer)")

	// kafkaのアドレス
	bootstrapServers = flag.String("bootstrapServers", "localhost:9092", "local kafka address")
	olded            = flag.Bool("olded", false, "Message acquisition method(true:all,false:latest)")
	offset           = sarama.OffsetOldest
)

type consumerGroupHandler struct{}

// SendMessage 送信メッセージ
type SendMessage struct {
	Message   string `json:"message"`
	Timestamp int64  `json:"timestamp"`
}

// ConsumedMessage 受信メッセージ
type ConsumedMessage struct {
	Message   string `json:"message"`
	Timestamp int64  `json:"timestamp"`
}

func main() {

	flag.Parse()
	if *bootstrapServers == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	switch *procType {
	case "Producer":
		procProducer()
	case "Consumer":
		procConsumer()
	default:
		break
	}
}

// Kafkaへのメッセージ送信処理（Producer）
func procProducer() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	brokers := strings.Split(*bootstrapServers, ",")

	// Sarama Config設定
	config := sarama.NewConfig()
	config.Producer.Return.Errors = true
	config.Producer.Return.Successes = true
	config.Producer.Retry.Max = 3
	config.Version = sarama.V0_10_2_0

	// Producer生成（非同期）
	producer, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		panic(err)
	}
	defer producer.AsyncClose()

	// プロデューサールーチン
	go func() {
	PRODUCER_FOR:
		for {
			time.Sleep(10000 * time.Millisecond)

			// Message
			timestamp := time.Now().UnixNano()
			send := &SendMessage{
				Message:   "Hello",
				Timestamp: timestamp,
			}

			jsBytes, err := json.Marshal(send)
			if err != nil {
				panic(err)
			}

			msg := &sarama.ProducerMessage{
				Topic: TopicName,
				Key:   sarama.StringEncoder(strconv.FormatInt(timestamp, 10)),
				Value: sarama.StringEncoder(string(jsBytes)),
			}

			// Message送信
			producer.Input() <- msg

			select {
			case <-producer.Successes():
				fmt.Println(fmt.Sprintf("success send. message: %s, timestamp: %d", send.Message, send.Timestamp))
			case err := <-producer.Errors():
				fmt.Println(fmt.Sprintf("fail send. reason: %v", err.Msg))
			case <-ctx.Done():
				break PRODUCER_FOR
			}
		}
	}()

	fmt.Println("go-kafka-Producer start.")

	<-signals

	fmt.Println("go-kafka-Producer stop.")
}

// Kafkaからのメッセージ取得（Consumer）
func procConsumer() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	brokers := strings.Split(*bootstrapServers, ",")

	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	// GroupIDを使用するため、バージョン（V0_10_2_0）を設定
	config.Version = sarama.V0_10_2_0

	if *olded {
		// brokerに溜まっている全てのメッセージを取得（デフォルト）
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	} else {
		// 最新のメッセージを取得
		config.Consumer.Offsets.Initial = sarama.OffsetNewest
	}

	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		panic(err)
	}
	defer func() { _ = client.Close() }()

	group, err := sarama.NewConsumerGroupFromClient(GroupID, client)
	if err != nil {
		panic(err)
	}
	defer func() { _ = group.Close() }()

	go func() {
		for err := range group.Errors() {
			fmt.Println("ERROR", err)
		}
	}()

	fmt.Println("go-kafka-Consumer start.")

	// コンシューマールーチン
	go func() {
	CONSUMER_FOR:
		for {
			topics := []string{TopicName}
			handler := consumerGroupHandler{}

			err := group.Consume(ctx, topics, handler)
			if err != nil {
				panic(err)
			}

			select {
			case <-ctx.Done():
				break CONSUMER_FOR
			}
		}
	}()

	<-signals
	fmt.Println("go-kafka-Producer stop.")
}

// consumerGroupHandler callback
func (consumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (consumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (h consumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	const commitPoint = int64(1)
	for msg := range claim.Messages() {
		// Consume側で最新のメッセージを取得した時にOFFSETを保持する。
		// 再度メッセージ取得時に、保持していたOFFSET +1を指定することにより、前回取得分から一つ新しいメッセージから取得することができる
		//if commitPoint > 0 && msg.Offset >= commitPoint {
		fmt.Printf("consumed message. message: %s, timestamp: %s offset:%d\n", msg.Value, msg.Timestamp, msg.Offset)
		//}
	}
	return nil
}
