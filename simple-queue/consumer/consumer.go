package main

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/Shopify/sarama"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	brokerList        = []string{"localhost:9092"}
	topic             = "rand_ints_simple"
	messageCountStart = 0
	ignoreOld         = kingpin.Flag("ignoreOld", "Ignore old messages").Default("false").Bool()
)

func main() {
	// Config
	kingpin.Parse()
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	master, err := sarama.NewConsumer(brokerList, config)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := master.Close(); err != nil {
			panic(err)
		}
	}()

	// Create consumer
	offset := sarama.OffsetOldest
	if *ignoreOld {
		offset = sarama.OffsetNewest
	}
	consumer, err := master.ConsumePartition(topic, 0, offset)
	if err != nil {
		panic(err)
	}

	// Set up OS Signal channel, and Done
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	doneCh := make(chan struct{})
	go func() {
		for {
			select {
			case err := <-consumer.Errors():
				fmt.Println(err)
			case msg := <-consumer.Messages():
				messageCountStart++
				fmt.Printf("%s <= offset(%d)\n", string(msg.Value), msg.Offset)
			case <-signals:
				fmt.Println("\nInterrupt detected")
				doneCh <- struct{}{}
			}
		}
	}()
	<-doneCh
	fmt.Println("Processed", messageCountStart, "messages")
}
