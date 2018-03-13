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
	topic      = "rand_ints"
)

func main() {
	// Config
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true
	config.Producer.Partitioner = func(string) sarama.Partitioner {
		return NewMod2Partitioner()
	}

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
			partition, offset, err := producer.SendMessage(msg)
			if err != nil {
				panic(err)
			}
			fmt.Printf("%d => partition(%d), offset(%d)\n", i, partition, offset)
		case <-signals:
			fmt.Println("\nInterrupt detected")
			return
		}
	}
}

// Custom Partitioner to illustrate partitions in action
type mod2Partitioner struct{}

func NewMod2Partitioner() sarama.Partitioner {
	return new(mod2Partitioner)
}

func (p *mod2Partitioner) Partition(message *sarama.ProducerMessage, numPartitions int32) (int32, error) {
	val, err := message.Value.Encode()
	if err != nil {
		return 0, err
	}

	i, err := strconv.Atoi(string(val))
	if err != nil {
		return 0, err
	}

	return int32(i % 2), nil
}

func (p *mod2Partitioner) RequiresConsistency() bool {
	return true
}
