# Kafka Consumer POC for Zee

This is a small POC on Python Kafka Consumer using `confluent_kafka` library for Zee use case

## Pre-requisites

- Python 3
- Virtualenv
- Confluent Cloud Credentials

## Install the dependencies

```bash
pip install -r requirements.txt
```

### Environment Variables

```bash
export KAFKA_BOOTSTRAP_SERVERS=
export KAFKA_API_KEY=
export KAFKA_API_SECRET=
```

## Consumer code

This code contains the `KafkaConsumer` class which reads a list of topics and consumes a consfigurable `max_messages`.

#### Create a instance of the KafkaConsumer class

Provide the topic list and the consumer configuration

```python
consumer1 = KafkaConsumer(topic_list, config)
```


#### Consume the max messages from the topic list

Provide the max messages and timeout(in seconds) value to the `consume` method. It returns the consumed messages

```python
messages = consumer1.consume(max_messages=5, timeout=10)
```
