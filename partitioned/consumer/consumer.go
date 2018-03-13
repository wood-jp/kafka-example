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
	topic             = "rand_ints"
	partition         = kingpin.Flag("partition", "Partition number").Default("0").Int32()
	messageCountStart = 0
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
	consumer, err := master.ConsumePartition(topic, *partition, sarama.OffsetNewest)
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
