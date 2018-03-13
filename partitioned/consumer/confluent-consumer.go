// Example channel-based high-level Apache Kafka consumer
// Modified from https://raw.githubusercontent.com/confluentinc/confluent-kafka-go/master/examples/consumer_channel_example/consumer_channel_example.go
package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	group = kingpin.Flag("group", "consumer group id").Default("").String()
)

func main() {
	kingpin.Parse()

	broker := "localhost:9092"
	topics := []string{"rand_ints"}

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":               broker,
		"group.id":                        *group,
		"session.timeout.ms":              6000,
		"go.events.channel.enable":        true,
		"go.application.rebalance.enable": true,
		"default.topic.config":            kafka.ConfigMap{"auto.offset.reset": "earliest"}})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
		os.Exit(1)
	}

	err = c.SubscribeTopics(topics, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to subscribe: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Created Consumer %v\n", c)

	doneCh := make(chan struct{})

	go func() {
		for {
			select {
			case sig := <-sigchan:
				fmt.Printf("\nCaught signal %v: terminating\n", sig)
				doneCh <- struct{}{}

			case ev := <-c.Events():
				switch e := ev.(type) {
				case kafka.AssignedPartitions:
					fmt.Fprintf(os.Stderr, "%% %v\n", e)
					c.Assign(e.Partitions)
				case kafka.RevokedPartitions:
					fmt.Fprintf(os.Stderr, "%% %v\n", e)
					c.Unassign()
				case *kafka.Message:
					fmt.Println(string(e.Value))
				case kafka.Error:
					fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
					doneCh <- struct{}{}
				}
			}
		}
	}()

	<-doneCh
	fmt.Printf("Closing consumer\n")
	c.Close()
}
