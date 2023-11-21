from confluent_kafka import Consumer, TopicPartition
import os
import json
import time


def process_messages(list_of_messages):
    ### Processing logic
    print("\nNo. of messages processed: " + str(len(list_of_messages)))
    return None


class KafkaConsumer:
    def __init__(self, topics: list, consumer_config: dict):
        self.topics = topics
        self.conf = consumer_config
        self.message_list = []

    def consume(self, max_messages=5, timeout=10):
        required_messages = max_messages

        print("Topics to be consumed:")
        print(self.topics)

        for topic in self.topics:
            ## Set topic name as the Consumer group id
            self.conf["group.id"] = topic

            ## Create the topic partition to consume data from. Assumes only one partition per topic. So, partition = 0
            topic_partition = TopicPartition(topic, 0)

            start_time = time.time()
    
            consumer = Consumer(self.conf)

            ## Assign the topic partition to the consumer
            consumer.assign([topic_partition])

            # ## Show the available high watermark offset
            # print("\nWatermark offsets for %s: " % topic)
            # print(consumer.get_watermark_offsets(topic_partition))

            ## Consume 5 messages or timeout after 10 seconds
            print("\nConsuming messages from " + str(topic))
            msg_consumed = 0
            try:
                while (required_messages > 0) and (time.time() - start_time <= timeout):
                    msg = consumer.poll(timeout=1.0)
                    if msg is None:
                        # print("\nWaiting for messages")
                        pass
                    elif msg.error():
                        print('\nError: {}'.format(msg.error()))
                    else:
                        # Proper message
                        print('%s [%d] at offset %d with key %s:' %
                                    (msg.topic(), msg.partition(), msg.offset(),
                                    str(msg.key())))
                        
                        ## Add the messages to the final message list
                        value = msg.value()
                        self.message_list.append(json.loads(value))
                        msg_consumed = msg_consumed + 1
                        
                        ## Update the required messages
                        required_messages = required_messages - 1

                        ## Commit the consumer message offset
                        consumer.commit()
            except KeyboardInterrupt:
                ## Exit on User Interruption
                print('\nAborted by user')
            finally:
                ## Close the consumer
                consumer.close()

            print("\nFinished Consuming " + str(msg_consumed) + " messages from " + topic)

            ## Exit the for loop if fetched the required messages
            if required_messages == 0:
                break
        
        ## Return the messages
        return self.message_list


if __name__ == "__main__":
    topic_list = ["topic_1", "topic_2", "topic_3"]

    ## Exclude group.id. It will be assigned inside the class
    config = {
        'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': os.getenv("KAFKA_API_KEY"),
        'sasl.password': os.getenv("KAFKA_API_SECRET"),
        'auto.offset.reset': 'earliest',
        'session.timeout.ms': 60000,
        'enable.auto.commit': False
    }

    ## Starting the process
    process_start_time = time.time()
    
    ## Create a instance
    consumer1 = KafkaConsumer(topic_list, config)
    
    ## Consumer required no. of messages
    messages = consumer1.consume(max_messages=5)

    print("\n")
    print("Messages consumed: ")
    print(messages)

    print("\nTotal Time Taken (in seconds) for Consumption: {}".format(time.time() - process_start_time) )

    ## Trigger the process message function
    process_messages(messages)