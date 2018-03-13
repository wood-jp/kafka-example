package main

import (
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"time"

	"github.com/Shopify/sarama"
)

var (
	brokerList = []string{"localhost:9092"}
	topic      = "rand_ints_simple"
)

func main() {
	// Config
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true

	// Create Synchronous Producer
	producer, err := sarama.NewSyncProducer(brokerList, config)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			panic(err)
		}
	}()

	// Set up OS signal channel, and Ticker
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	t := time.NewTicker(time.Millisecond * 800)
	defer func() {
		t.Stop()
	}()

	// Produce messages until interrupted
	for {
		select {
		case <-t.C:
			i := rand.Intn(10)
			msg := &sarama.ProducerMessage{
				Topic: topic,
				Value: sarama.StringEncoder(strconv.Itoa(i)),
			}
			_, offset, err := producer.SendMessage(msg)
			if err != nil {
				panic(err)
			}
			fmt.Printf("%d => offset(%d)\n", i, offset)
		case <-signals:
			fmt.Println("\nInterrupt detected")
			return
		}
	}
}
