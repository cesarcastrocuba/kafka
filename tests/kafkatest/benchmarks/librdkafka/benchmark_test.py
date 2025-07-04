# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from ducktape.mark import matrix
from ducktape.mark import parametrize
from ducktape.mark.resource import cluster
from ducktape.services.service import Service

from kafkatest.services.kafka import KafkaService, quorum
from kafkatest.services.performance import throughput, latency, compute_aggregate_throughput
from kafkatest.version import DEV_BRANCH, KafkaVersion
from kafkatest.benchmarks.core.benchmark_test import Benchmark

from kafkatest.benchmarks.librdkafka.producer_consumer_performance import LibrdKafkaProducerConsumerPerformance
from kafkatest.benchmarks.librdkafka.end_to_end_latency import LibrdKafkaEndToEndLatency
from kafkatest.benchmarks.librdkafka.setup_utils import setup_librdkafka, configure_security_settings

# Reuse the same topic names from the original benchmark
from kafkatest.benchmarks.core.benchmark_test import TOPIC_REP_ONE, TOPIC_REP_THREE
DEFAULT_RECORD_SIZE = 100  # bytes

class LibrdkafkaBenchmark(Benchmark):
    """A benchmark of librdkafka producer/consumer performance. This replicates the test
    run for Java clients but using librdkafka instead.
    
    This class inherits from KafkaPerformanceTest to reuse common configuration and setup.
    """
    def __init__(self, test_context):
        # Call the parent class constructor to initialize common variables
        super(LibrdkafkaBenchmark, self).__init__(test_context)
        self.logger.info("Installing librdkafka on all nodes")
        nodes = self.test_context.cluster.nodes
        success = setup_librdkafka(nodes)
        if not success:
            raise RuntimeError("Failed to install librdkafka on one or more nodes")        

    @cluster(num_nodes=13)
    def test_load_librdkafka(self):
        self.logger.info("Load Librdkafka in nodes")
        
    @cluster(num_nodes=9)
    @parametrize(acks=1, topic=TOPIC_REP_ONE, metadata_quorum=quorum.isolated_kraft)
    @parametrize(acks=1, topic=TOPIC_REP_THREE, metadata_quorum=quorum.isolated_kraft)
    @parametrize(acks=-1, topic=TOPIC_REP_THREE, metadata_quorum=quorum.isolated_kraft)
    @matrix(acks=[1], topic=[TOPIC_REP_THREE], message_size=[10, 100, 1000, 10000, 100000],
            compression_type=["none", "snappy"], security_protocol=['SSL'], tls_version=['TLSv1.2', 'TLSv1.3'], metadata_quorum=[quorum.isolated_kraft])
    @matrix(acks=[1], topic=[TOPIC_REP_THREE], message_size=[10, 100, 1000, 10000, 100000],
            compression_type=["none", "snappy"], security_protocol=['PLAINTEXT'], metadata_quorum=[quorum.isolated_kraft])
    @parametrize(acks=1, topic=TOPIC_REP_THREE, num_producers=3, metadata_quorum=quorum.isolated_kraft)
    def test_producer_throughput(self, acks, topic, num_producers=1, message_size=DEFAULT_RECORD_SIZE,
                                 compression_type="none", security_protocol='PLAINTEXT', tls_version=None, client_version=str(DEV_BRANCH),
                                 broker_version=str(DEV_BRANCH), metadata_quorum=quorum.isolated_kraft):
        """
        Setup: 3 node kafka cluster
        Produce ~128MB worth of messages to a topic with 6 partitions. Required acks, topic replication factor,
        security protocol and message size are varied depending on arguments injected into this test.

        Collect and return aggregate throughput statistics after all messages have been acknowledged.
        (This uses rdkafka_performance instead of ProducerPerformance.java)
        """
        client_version = KafkaVersion(client_version)
        broker_version = KafkaVersion(broker_version)
        self.validate_versions(client_version, broker_version)
        self.start_kafka(security_protocol, security_protocol, broker_version, tls_version)

        # Set up the cluster with librdkafka and tools
        self.logger.info("Setting up cluster with librdkafka and tools")

        # Always generate the same total amount of data
        nrecords = int(self.target_data_size / message_size)
      
        # Convert acks to librdkafka format
        rdkafka_acks = "all" if acks == -1 else str(acks)
        
        # Prepare settings for librdkafka
        settings = {
            'request.required.acks': rdkafka_acks,
            'compression.type': compression_type,
            'batch.size': self.batch_size,
            'queue.buffering.max.kbytes': self.buffer_memory // 1024,  # Convert to KB
            'queue.buffering.max.messages':1000000, 
            'queue.buffering.max.ms':0,
            'message.timeout.ms':120000,
        }
        
        # Add security settings if needed
        security_settings = configure_security_settings(security_protocol, tls_version)
        settings.update(security_settings)

        self.producer = LibrdKafkaProducerConsumerPerformance(
            self.test_context, num_producers, self.kafka, topic=topic,
            num_records=nrecords, record_size=message_size, throughput=-1,
            mode="producer", version=client_version, settings=settings)
        
        self.producer.run()
        return compute_aggregate_throughput(self.producer, self.execution_id)

    @cluster(num_nodes=7)
    @matrix(security_protocol=['SSL'], interbroker_security_protocol=['PLAINTEXT'], tls_version=['TLSv1.2', 'TLSv1.3'],
            compression_type=["none", "snappy"], metadata_quorum=[quorum.isolated_kraft])
    @matrix(security_protocol=['PLAINTEXT'], compression_type=["none", "snappy"], metadata_quorum=[quorum.isolated_kraft])
    def test_long_term_producer_throughput(self, compression_type="none",
                                           security_protocol='PLAINTEXT', tls_version=None,
                                           interbroker_security_protocol=None, client_version=str(DEV_BRANCH),
                                           broker_version=str(DEV_BRANCH), metadata_quorum=quorum.isolated_kraft):
        """
        Setup: 3 node kafka cluster
        Produce 10e6 100 byte messages to a topic with 6 partitions, replication-factor 3, and acks=1.

        Collect and return aggregate throughput statistics after all messages have been acknowledged.

        (This uses rdkafka_performance instead of ProducerPerformance.java)
        """
        client_version = KafkaVersion(client_version)
        broker_version = KafkaVersion(broker_version)
        self.validate_versions(client_version, broker_version)
        if interbroker_security_protocol is None:
            interbroker_security_protocol = security_protocol
        self.start_kafka(security_protocol, interbroker_security_protocol, broker_version, tls_version)
        
        # Prepare settings for librdkafka
        settings = {
            'request.required.acks': '1',
            'compression.type': compression_type,
            'batch.size': self.batch_size,
            'queue.buffering.max.kbytes': self.buffer_memory // 1024,  # Convert to KB
            'queue.buffering.max.messages':1000000, 
            'queue.buffering.max.ms':0,
            'message.timeout.ms':120000,
        }
        
        # Add security settings if needed
        security_settings = configure_security_settings(security_protocol, tls_version)
        settings.update(security_settings)
        
        self.producer = LibrdKafkaProducerConsumerPerformance(
            self.test_context, 1, self.kafka,
            topic=TOPIC_REP_THREE, num_records=self.msgs_large, record_size=DEFAULT_RECORD_SIZE,
            throughput=-1, mode="producer", version=client_version, settings=settings, intermediate_stats=True
        )
        
        self.producer.run()

        summary = ["Throughput over long run, data > memory:"]
        data = {}
        # Try to break it into 5 blocks, but fall back to a smaller number if
        # there aren't even 5 elements
        block_size = max(len(self.producer.stats[0]) // 5, 1)
        nblocks = len(self.producer.stats[0]) // block_size

        for i in range(nblocks):
            subset = self.producer.stats[0][i*block_size:min((i+1)*block_size, len(self.producer.stats[0]))]
            if not subset:
                summary.append(" Time block %d: (empty)" % i)
                data[i] = None
            else:
                records_per_sec = sum([stat['records_per_sec'] for stat in subset])/float(len(subset))
                mb_per_sec = sum([stat['mbps'] for stat in subset])/float(len(subset))
                records = sum(stat.get('records', 0) for stat in subset)
                duration_ms = sum(stat.get('duration_ms', 0) for stat in subset)

                summary.append(" Time block %d: %f rec/sec (%f MB/s)" % (i, records_per_sec, mb_per_sec))
                data[i] = throughput(records_per_sec, mb_per_sec, records, duration_ms,
                                     self.execution_id)

        self.logger.info("\n".join(summary))
        return data

    @cluster(num_nodes=8)
    @matrix(security_protocol=['SSL'], interbroker_security_protocol=['PLAINTEXT'], tls_version=['TLSv1.2', 'TLSv1.3'],
            compression_type=["none", "snappy"], metadata_quorum=[quorum.isolated_kraft])
    @matrix(security_protocol=['PLAINTEXT'], compression_type=["none", "snappy"], metadata_quorum=[quorum.isolated_kraft])
    # @matrix(security_protocol=['SASL_PLAINTEXT', 'SASL_SSL'], compression_type=["none", "snappy"], metadata_quorum=[quorum.isolated_kraft])
    def test_end_to_end_latency(self, compression_type="none", security_protocol="PLAINTEXT", tls_version=None,
                                interbroker_security_protocol=None, client_version=str(DEV_BRANCH),
                                broker_version=str(DEV_BRANCH), metadata_quorum=quorum.isolated_kraft):
        """
        Setup: 3 node kafka cluster
        Produce (acks = 1) and consume 10e3 messages to a topic with 6 partitions and replication-factor 3,
        measuring the latency between production and consumption of each message.

        Return aggregate latency statistics.

        (This uses our C implementation instead of EndToEndLatency.java)
        """
        client_version = KafkaVersion(client_version)
        broker_version = KafkaVersion(broker_version)
        self.validate_versions(client_version, broker_version)
        if interbroker_security_protocol is None:
            interbroker_security_protocol = security_protocol
        self.start_kafka(security_protocol, interbroker_security_protocol, broker_version, tls_version)
        self.logger.info("BENCHMARK: End to end latency")
        
        # Prepare settings for librdkafka
        settings = {
            'request.required.acks': '1',
            'compression.type': compression_type,
            'batch.size': 1,  # Small batch size to measure latency
            'queue.buffering.max.kbytes': self.buffer_memory // 1024,  # Convert to KB
            'queue.buffering.max.messages':1000000, 
            'queue.buffering.max.ms':0,
            'message.timeout.ms':120000,
            'retries': 0,  # Avoid retry delays for latency testing

            'auto.offset.reset': 'latest',
            'group.id': 'test-consumer-group',
        }
        
        # Add security settings if needed
        security_settings = configure_security_settings(security_protocol, tls_version)
        settings.update(security_settings)

        self.perf = LibrdKafkaEndToEndLatency(
            self.test_context, 1, self.kafka,
            topic=TOPIC_REP_THREE, num_records=10000, version=client_version,
            settings=settings
        )
        
        self.perf.run()
        return latency(self.perf.results[0]['latency_50th_ms'], 
                      self.perf.results[0]['latency_99th_ms'], 
                      self.perf.results[0]['latency_999th_ms'],
                      self.execution_id)

    @cluster(num_nodes=8)
    @matrix(security_protocol=['SSL'], interbroker_security_protocol=['PLAINTEXT'], tls_version=['TLSv1.2', 'TLSv1.3'],
            compression_type=["none", "snappy"], metadata_quorum=[quorum.isolated_kraft])
    @matrix(security_protocol=['PLAINTEXT'], compression_type=["none", "snappy"], metadata_quorum=[quorum.isolated_kraft])
    def test_producer_and_consumer(self, compression_type="none", security_protocol="PLAINTEXT", tls_version=None,
                                   interbroker_security_protocol=None,
                                   client_version=str(DEV_BRANCH), broker_version=str(DEV_BRANCH), metadata_quorum=quorum.isolated_kraft):
        """
        Setup: 3 node kafka cluster
        Concurrently produce and consume 10e6 messages with a single producer and a single consumer,

        Return aggregate throughput statistics for both producer and consumer.

        (This uses rdkafka_performance instead of ProducerPerformance.java and ConsumerPerformance.java)
        """
        client_version = KafkaVersion(client_version)
        broker_version = KafkaVersion(broker_version)
        self.validate_versions(client_version, broker_version)
        if interbroker_security_protocol is None:
            interbroker_security_protocol = security_protocol
        self.start_kafka(security_protocol, interbroker_security_protocol, broker_version, tls_version)
        num_records = 10 * 1000 * 1000  # 10e6

        # Add security settings if needed
        security_settings = configure_security_settings(security_protocol, tls_version)
        
        # Prepare producer settings
        producer_settings = {
            'request.required.acks': '1',
            'compression.codec': compression_type,
            'batch.size': self.batch_size,
            'queue.buffering.max.kbytes': self.buffer_memory // 1024,  # Convert to KB
            'queue.buffering.max.messages':1000000, 
            'queue.buffering.max.ms':0,
            'message.timeout.ms':120000,            
        }
        
        producer_settings.update(security_settings)

        # Prepare consumer settings
        consumer_settings = {
            'auto.offset.reset': 'earliest',
            'group.id': 'test-consumer-group',            
            'socket.receive.buffer.bytes': '2097152',  # Align with Java's receive.buffer.bytes
            'check.crcs': 'false',                     # Align with Java's check.crcs
            'session.timeout.ms': '45000',             # Align with Java's session.timeout.ms
            'fetch.wait.max.ms': '500'                 # Align with Java's fetch.max.wait.ms
        }

        consumer_settings.update(security_settings)
        
        self.producer = LibrdKafkaProducerConsumerPerformance(
            self.test_context, 1, self.kafka,
            topic=TOPIC_REP_THREE,
            num_records=num_records, record_size=DEFAULT_RECORD_SIZE, throughput=-1,
            mode="producer", version=client_version, settings=producer_settings
        )
        
        self.consumer = LibrdKafkaProducerConsumerPerformance(
            self.test_context, 1, self.kafka,
            topic=TOPIC_REP_THREE,
            num_records=num_records,
            mode="consumer", version=client_version, settings=consumer_settings
        )
        
        # Setup librdkafka on all nodes
        self.logger.info("Setting up librdkafka for producer and consumer")
        setup_success = setup_librdkafka(nodes=self.producer.nodes + self.consumer.nodes)
        if not setup_success:
            raise Exception("Failed to set up librdkafka on some nodes")
        
        # Mark both services as already set up
        self.producer.librdkafka_setup = True
        self.consumer.librdkafka_setup = True
        
        Service.run_parallel(self.producer, self.consumer)

        data = {
            "producer": compute_aggregate_throughput(self.producer, self.execution_id),
            "consumer": compute_aggregate_throughput(self.consumer, self.execution_id)
        }
        summary = [
            "Producer + consumer:",
            str(data)]
        self.logger.info("\n".join(summary))
        return data

    @cluster(num_nodes=8)
    @matrix(security_protocol=['SSL'], interbroker_security_protocol=['PLAINTEXT'], tls_version=['TLSv1.2', 'TLSv1.3'],
            compression_type=["none", "snappy"], metadata_quorum=[quorum.isolated_kraft])
    @matrix(security_protocol=['PLAINTEXT'], compression_type=["none", "snappy"], metadata_quorum=[quorum.isolated_kraft])
    def test_consumer_throughput(self, compression_type="none", security_protocol="PLAINTEXT", tls_version=None,
                                 interbroker_security_protocol=None, num_consumers=1,
                                 client_version=str(DEV_BRANCH), broker_version=str(DEV_BRANCH), metadata_quorum=quorum.isolated_kraft):
        """
        Consume 10e6 100-byte messages with 1 or more consumers from a topic with 6 partitions
        and report throughput.
        """
        client_version = KafkaVersion(client_version)
        broker_version = KafkaVersion(broker_version)
        self.validate_versions(client_version, broker_version)
        if interbroker_security_protocol is None:
            interbroker_security_protocol = security_protocol
        self.start_kafka(security_protocol, interbroker_security_protocol, broker_version, tls_version)
        num_records = 10 * 1000 * 1000  # 10e6

        # Prepare producer settings
        producer_settings = {
            'request.required.acks': '1',
            'compression.type': compression_type,
            'batch.size': self.batch_size,            
            'queue.buffering.max.kbytes': self.buffer_memory // 1024,  # Convert to KB
            'queue.buffering.max.messages':1000000, 
            'queue.buffering.max.ms':0,  # Align with Java's linger.ms
            'message.timeout.ms':120000, # Align with Java's delivery.timeout.ms
        }
        
        # Add security settings if needed
        security_settings = configure_security_settings(security_protocol, tls_version)
        producer_settings.update(security_settings)

        # seed kafka w/messages
        self.producer = LibrdKafkaProducerConsumerPerformance(
            self.test_context, 1, self.kafka,
            topic=TOPIC_REP_THREE,
            num_records=num_records, record_size=DEFAULT_RECORD_SIZE, throughput=-1,
            mode="producer", version=client_version, settings=producer_settings
        )
        
        self.producer.run()

        # Prepare consumer settings
        consumer_settings = {
            'auto.offset.reset': 'earliest',
            'group.id': 'test-consumer-group',
            'socket.receive.buffer.bytes': '2097152',  # Align with Java's receive.buffer.bytes
            'check.crcs': 'false',                     # Align with Java's check.crcs
            'session.timeout.ms': '45000',             # Align with Java's session.timeout.ms
            'fetch.wait.max.ms': '500'                 # Align with Java's fetch.max.wait.ms
        }

        # Add security settings if needed
        consumer_settings.update(security_settings)
        
        # consume
        self.consumer = LibrdKafkaProducerConsumerPerformance(
            self.test_context, num_consumers, self.kafka,
            topic=TOPIC_REP_THREE,
            num_records=num_records, record_size=DEFAULT_RECORD_SIZE, throughput=-1,
            mode="consumer", settings=consumer_settings
        )
        
        self.consumer.run()
        return compute_aggregate_throughput(self.consumer, self.execution_id)



     
        