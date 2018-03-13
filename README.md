# kafka-example

Sample code used during talk given at [KW Go Developers](https://www.meetup.com/Golang-KW/ "KW Go Developers Meetup") on 13 Mar 2018.

Code shamlessly borrowed and modified from:
- https://github.com/vsouza/go-kafka-example
- https://github.com/confluentinc/confluent-kafka-go/tree/master/examples

To run all the examples, you'll need to install:
- Kafka
  - Personally I found [this page](https://hevodata.com/blog/how-to-set-up-kafka-on-ubuntu-16-04/) very helpful
- [sarama](https://github.com/Shopify/sarama)
- [confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go)
  - requires [librdkafka](https://github.com/edenhill/librdkafka)

Additionally, you'll need to ensure that the topic `rand_ints` has at least 2 partitions.
If the topic doesn't already exist, this (or similar) should do the trick:
```
/opt/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic rand_ints
```

