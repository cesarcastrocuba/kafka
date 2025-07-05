/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

 #include <stdio.h>
 #include <stdlib.h>
 #include <string.h>
 #include <time.h>
 #include <unistd.h>
 #include <librdkafka/rdkafka.h>
 #include <sys/time.h>
 #include <signal.h>
 #include <ctype.h>
 #include <errno.h>
 
 #define POLL_TIMEOUT_MS 60000
 #define DEFAULT_TOPIC_PARTITIONS 1
 #define DEFAULT_REPLICATION_FACTOR 1
 #define DEFAULT_MESSAGE_SIZE 100
 #define MAX_CONFIG_LINE 512
 #define MAX_CONFIG_VALUE 256
 
 // Global variables for clean shutdown
 static volatile sig_atomic_t run = 1;
 
 typedef struct {
     char* brokers;
     char* topic;
     int num_messages;
     int message_size;
     char* config_file;
 } config_t;
 
 // Signal handler for clean shutdown
 static void stop(int sig) {
     run = 0;
 }
 
 // Function to get current time in nanoseconds
 static long long current_time_ns() {
     struct timespec ts;
     clock_gettime(CLOCK_MONOTONIC, &ts);
     return (long long)ts.tv_sec * 1000000000LL + ts.tv_nsec;
 }
 
 // Function to generate random bytes
 static void random_bytes(char *buf, size_t size) {
     for (size_t i = 0; i < size; i++) {
         buf[i] = 'A' + (rand() % 26);
     }
 }
 
 // Function to compare two messages
 static int compare_messages(const char *sent, const char *received, size_t size) {
     return memcmp(sent, received, size) == 0;
 }
 
 // Message delivery report callback
 static void dr_msg_cb(rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque) {
     if (rkmessage->err) {
         fprintf(stderr, "Message delivery failed: %s\n", rd_kafka_err2str(rkmessage->err));
     }
 }
 
 // Function to parse configuration file
 static void parse_config_file(rd_kafka_conf_t *conf, const char *config_file, int is_producer) {
    if (!config_file) return;

    FILE *fp = fopen(config_file, "r");
    if (!fp) {
        fprintf(stderr, "Failed to open config file: %s (%s)\n", config_file, strerror(errno));
        return;
    }

    char line[MAX_CONFIG_LINE];
    char key[MAX_CONFIG_VALUE];
    char value[MAX_CONFIG_VALUE];

    while (fgets(line, sizeof(line), fp)) {
        // Skip comments and empty lines
        if (line[0] == '#' || line[0] == '\n') continue;

        // Parse key=value
        if (sscanf(line, "%255[^=]=%255[^\n]", key, value) == 2) {
            // Trim whitespace from key
            char *p = key + strlen(key) - 1;
            while (p >= key && isspace(*p)) *p-- = '\0';
            
            // Trim leading whitespace from value
            p = value;
            while (*p && isspace(*p)) p++;
            
            // Skip settings based on client type
            if (is_producer) {
                if (strcmp(key, "check.crcs") == 0 ||
                    strcmp(key, "fetch.message.max.bytes") == 0 ||
                    strcmp(key, "fetch.wait.max.ms") == 0 ||
                    strcmp(key, "enable.auto.commit") == 0 ||
                    strcmp(key, "group.id") == 0 ||                    
                    strcmp(key, "auto.offset.reset") == 0) {
                    continue;
                }
            } else {
                if (strcmp(key, "retries") == 0 ||
                    strcmp(key, "message.timeout.ms") == 0 ||
                    strcmp(key, "queue.buffering.max.ms") == 0 ||
                    strcmp(key, "queue.buffering.max.messages") == 0 ||
                    strcmp(key, "queue.buffering.max.kbytes") == 0 ||                    
                    strcmp(key, "batch.size") == 0 ||
                    strcmp(key, "compression.type") == 0 ||
                    strcmp(key, "request.required.acks") == 0) {
                    continue;
                }
            }

            char errstr[512];
            if (rd_kafka_conf_set(conf, key, p, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                fprintf(stderr, "Config error: %s=%s: %s\n", key, p, errstr);
            }
        }
    }
    fclose(fp);
}
 // Function to create and configure a Kafka producer
 static rd_kafka_t *create_producer(const config_t *config) {
     rd_kafka_t *producer;
     rd_kafka_conf_t *conf = rd_kafka_conf_new();
     char errstr[512];
     
     // Set bootstrap.servers
     if (rd_kafka_conf_set(conf, "bootstrap.servers", config->brokers, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
         fprintf(stderr, "Error setting bootstrap.servers: %s\n", errstr);
         rd_kafka_conf_destroy(conf);
         return NULL;
     }
     
     // Set delivery report callback
     rd_kafka_conf_set_dr_msg_cb(conf, dr_msg_cb);
     
     // Set linger.ms to 0 for synchronous writes
     if (rd_kafka_conf_set(conf, "linger.ms", "0", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
         fprintf(stderr, "Error setting linger.ms: %s\n", errstr);
         rd_kafka_conf_destroy(conf);
         return NULL;
     }
     
     // Load additional configuration from file
     parse_config_file(conf, config->config_file, 1);
     
     // Create producer
     producer = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
     if (!producer) {
         fprintf(stderr, "Failed to create producer: %s\n", errstr);
         return NULL;
     }
     
     return producer;
 }
 
 // Function to create and configure a Kafka consumer
 static rd_kafka_t *create_consumer(const config_t *config) {
     rd_kafka_t *consumer;
     rd_kafka_conf_t *conf = rd_kafka_conf_new();
     char errstr[512];
     
     // Set bootstrap.servers
     if (rd_kafka_conf_set(conf, "bootstrap.servers", config->brokers, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
         fprintf(stderr, "Error setting bootstrap.servers: %s\n", errstr);
         rd_kafka_conf_destroy(conf);
         return NULL;
     }
     
     // Set group.id
     char group_id[128];
     snprintf(group_id, sizeof(group_id), "e2e-latency-group-%ld", time(NULL));
     if (rd_kafka_conf_set(conf, "group.id", group_id, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
         fprintf(stderr, "Error setting group.id: %s\n", errstr);
         rd_kafka_conf_destroy(conf);
         return NULL;
     }
     
     // Set auto.offset.reset to latest
     if (rd_kafka_conf_set(conf, "auto.offset.reset", "latest", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
         fprintf(stderr, "Error setting auto.offset.reset: %s\n", errstr);
         rd_kafka_conf_destroy(conf);
         return NULL;
     }
     
     // Disable auto commit
     if (rd_kafka_conf_set(conf, "enable.auto.commit", "false", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
         fprintf(stderr, "Error setting enable.auto.commit: %s\n", errstr);
         rd_kafka_conf_destroy(conf);
         return NULL;
     }
     
     // Set fetch.wait.max.ms to 0 to avoid temporal batching
     if (rd_kafka_conf_set(conf, "fetch.wait.max.ms", "0", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
         fprintf(stderr, "Error setting fetch.wait.max.ms: %s\n", errstr);
         rd_kafka_conf_destroy(conf);
         return NULL;
     }
     
     // Load additional configuration from file
     parse_config_file(conf, config->config_file, 0);
     
     // Create consumer
     consumer = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
     if (!consumer) {
         fprintf(stderr, "Failed to create consumer: %s\n", errstr);
         return NULL;
     }
     
     // Subscribe to topic
     rd_kafka_topic_partition_list_t *topics = rd_kafka_topic_partition_list_new(1);
     rd_kafka_topic_partition_list_add(topics, config->topic, RD_KAFKA_PARTITION_UA);
     
     rd_kafka_resp_err_t err = rd_kafka_subscribe(consumer, topics);
     if (err) {
         fprintf(stderr, "Failed to subscribe to topic: %s\n", rd_kafka_err2str(err));
         rd_kafka_topic_partition_list_destroy(topics);
         rd_kafka_destroy(consumer);
         return NULL;
     }
     
     rd_kafka_topic_partition_list_destroy(topics);
     return consumer;
 }
 
 // Function to ensure topic exists
 static int ensure_topic_exists(const config_t *config) {
     rd_kafka_t *admin;
     rd_kafka_conf_t *conf = rd_kafka_conf_new();
     char errstr[512];
     
     // Set bootstrap.servers
     if (rd_kafka_conf_set(conf, "bootstrap.servers", config->brokers, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
         fprintf(stderr, "Error setting bootstrap.servers: %s\n", errstr);
         rd_kafka_conf_destroy(conf);
         return 0;
     }
     
     // Load additional configuration from file
     parse_config_file(conf, config->config_file, 1);
     
     // Create admin client
     admin = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
     if (!admin) {
         fprintf(stderr, "Failed to create admin client: %s\n", errstr);
         return 0;
     }
     
     // Check if topic exists
     rd_kafka_metadata_t *metadata;
     rd_kafka_resp_err_t err = rd_kafka_metadata(admin, 0, NULL, &metadata, 5000);
     if (err) {
         fprintf(stderr, "Failed to get metadata: %s\n", rd_kafka_err2str(err));
         rd_kafka_destroy(admin);
         return 0;
     }
     
     int topic_exists = 0;
     for (int i = 0; i < metadata->topic_cnt; i++) {
         if (strcmp(metadata->topics[i].topic, config->topic) == 0) {
             topic_exists = 1;
             break;
         }
     }
     
     if (!topic_exists) {
         printf("Topic \"%s\" does not exist. Will create topic with %d partition(s) and replication factor = %d\n",
                config->topic, DEFAULT_TOPIC_PARTITIONS, DEFAULT_REPLICATION_FACTOR);
         
         // Create topic
         rd_kafka_NewTopic_t *new_topic = rd_kafka_NewTopic_new(
             config->topic, DEFAULT_TOPIC_PARTITIONS, DEFAULT_REPLICATION_FACTOR,
             errstr, sizeof(errstr));
         
         if (!new_topic) {
             fprintf(stderr, "Failed to create NewTopic object: %s\n", errstr);
             rd_kafka_metadata_destroy(metadata);
             rd_kafka_destroy(admin);
             return 0;
         }
         
         rd_kafka_queue_t *queue = rd_kafka_queue_new(admin);
         rd_kafka_CreateTopics(admin, &new_topic, 1, NULL, queue);
         
         rd_kafka_event_t *event = rd_kafka_queue_poll(queue, 10000);
         if (!event || rd_kafka_event_type(event) != RD_KAFKA_EVENT_CREATETOPICS_RESULT) {
             fprintf(stderr, "Failed to create topic: wrong event type\n");
             if (event) rd_kafka_event_destroy(event);
             rd_kafka_queue_destroy(queue);
             rd_kafka_NewTopic_destroy(new_topic);
             rd_kafka_metadata_destroy(metadata);
             rd_kafka_destroy(admin);
             return 0;
         }
         
         const rd_kafka_CreateTopics_result_t *result = rd_kafka_event_CreateTopics_result(event);
         size_t result_cnt;
         const rd_kafka_topic_result_t **results = rd_kafka_CreateTopics_result_topics(result, &result_cnt);
         
         for (size_t i = 0; i < result_cnt; i++) {
             const char *topic_name = rd_kafka_topic_result_name(results[i]);
             rd_kafka_resp_err_t err = rd_kafka_topic_result_error(results[i]);
             if (err) {
                 fprintf(stderr, "Failed to create topic %s: %s\n", topic_name, 
                        rd_kafka_topic_result_error_string(results[i]));
             } else {
                 printf("Successfully created topic %s\n", topic_name);
                 topic_exists = 1;
             }
         }
         
         rd_kafka_event_destroy(event);
         rd_kafka_queue_destroy(queue);
         rd_kafka_NewTopic_destroy(new_topic);
     }
     
     rd_kafka_metadata_destroy(metadata);
     rd_kafka_destroy(admin);
     
     return topic_exists;
 }
 
 // Function to print usage information
 static void print_usage(const char *program_name) {
     fprintf(stderr, "Usage: %s <brokers> <topic> <num_messages> <message_size> [config_file]\n", program_name);
     fprintf(stderr, "  brokers       - Kafka broker list (host:port,host:port,...)\n");
     fprintf(stderr, "  topic         - Topic to produce/consume from\n");
     fprintf(stderr, "  num_messages  - Number of messages to send\n");
     fprintf(stderr, "  message_size  - Size of each message in bytes\n");     
     fprintf(stderr, "  config_file   - Optional configuration file (key=value format)\n");     
 }
 
 // Function to parse command line arguments
 static int parse_args(int argc, char *argv[], config_t *config) {
     if (argc < 6 || argc > 8) {
         print_usage(argv[0]);
         return 0;
     }
 
     config->brokers = argv[1];
     config->topic = argv[2];
     config->num_messages = atoi(argv[3]);
     config->message_size = atoi(argv[4]);     
     config->config_file = (argc >= 6) ? argv[5] : NULL;
 
     // Validate required arguments
     if (config->num_messages <= 0) {
         fprintf(stderr, "num_messages must be a positive integer\n");
         return 0;
     }
 
     if (config->message_size <= 0) {
         fprintf(stderr, "message_size must be a positive integer\n");
         return 0;
     }
 
     return 1;
 }
 
 
 // Main function
 int main(int argc, char *argv[]) {
     config_t config;
     rd_kafka_t *producer = NULL;
     rd_kafka_t *consumer = NULL;
     long long *latencies = NULL;
     char *message = NULL;
     
     // Parse command line arguments
     if (!parse_args(argc, argv, &config)) {
         return 1;
     }
 
     // Initialize random seed
     srand(0);
     
     // Set up signal handler
     signal(SIGINT, stop);
     signal(SIGTERM, stop);
     
     // Ensure topic exists
     if (!ensure_topic_exists(&config)) {
         fprintf(stderr, "Failed to ensure topic exists\n");
         return 1;
     }
     
     // Create producer
     producer = create_producer(&config);
     if (!producer) {
         fprintf(stderr, "Failed to create producer\n");
         return 1;
     }
     
     // Create consumer
     consumer = create_consumer(&config);
     if (!consumer) {
         fprintf(stderr, "Failed to create consumer\n");
         rd_kafka_destroy(producer);
         return 1;
     }
     
     // Allocate memory for latencies
     latencies = calloc(config.num_messages, sizeof(long long));
     if (!latencies) {
         fprintf(stderr, "Failed to allocate memory for latencies\n");
         goto cleanup;
     }
     
     // Allocate memory for message
     message = malloc(config.message_size);
     if (!message) {
         fprintf(stderr, "Failed to allocate memory for message\n");
         goto cleanup;
     }
     
     double total_time = 0.0;
     
     // Run the test
     for (int i = 0; i < config.num_messages && run; i++) {
         // Generate random message
         random_bytes(message, config.message_size);
         
         // Start timing
         long long start_time = current_time_ns();
         
         // Produce message
         rd_kafka_resp_err_t err = rd_kafka_producev(
             producer,
             RD_KAFKA_V_TOPIC(config.topic),
             RD_KAFKA_V_PARTITION(RD_KAFKA_PARTITION_UA),
             RD_KAFKA_V_VALUE(message, config.message_size),
             RD_KAFKA_V_OPAQUE(NULL),
             RD_KAFKA_V_END
         );
         
         if (err) {
             fprintf(stderr, "Failed to produce message %d: %s\n", i, rd_kafka_err2str(err));
             continue;
         }
         
         // Wait for message to be delivered
         while (rd_kafka_outq_len(producer) > 0 && run) {
             rd_kafka_poll(producer, 100);
         }
         
         // Consume message
         rd_kafka_message_t *msg = NULL;
         int received = 0;
         
         while (!received && run) {
             msg = rd_kafka_consumer_poll(consumer, POLL_TIMEOUT_MS);
             if (msg) {
                 if (msg->err) {
                     fprintf(stderr, "Consumer error: %s\n", rd_kafka_message_errstr(msg));
                     rd_kafka_message_destroy(msg);
                 } else if (msg->len == config.message_size && 
                            compare_messages(message, msg->payload, config.message_size)) {
                     received = 1;
                     rd_kafka_message_destroy(msg);
                 } else {
                     // Not our message, continue polling
                     rd_kafka_message_destroy(msg);
                 }
             } else {
                 fprintf(stderr, "Consumer poll timeout for message %d\n", i);
                 break;
             }
         }
         
         if (!received) {
             fprintf(stderr, "Failed to receive message %d\n", i);
             continue;
         }
         
         // End timing
         long long end_time = current_time_ns();
         long long elapsed = end_time - start_time;
         
         // Store latency in milliseconds
         latencies[i] = elapsed / 1000000;
         total_time += elapsed;
         
         // Report progress
         if (i % 1000 == 0) {
             printf("%d\t%.3f\n", i, elapsed / 1000000.0);
         }
     }
     
     // Calculate statistics
     double avg_latency = total_time / (config.num_messages * 1000000.0);
     
     // Sort latencies for percentile calculation (simple bubble sort for small N)
     for (int i = 0; i < config.num_messages - 1; i++) {
         for (int j = 0; j < config.num_messages - i - 1; j++) {
             if (latencies[j] > latencies[j + 1]) {
                 long long temp = latencies[j];
                 latencies[j] = latencies[j + 1];
                 latencies[j + 1] = temp;
             }
         }
     }
     
     // Calculate percentiles
     int p50 = latencies[(int)(config.num_messages * 0.5)];
     int p99 = latencies[(int)(config.num_messages * 0.99)];
     int p999 = latencies[(int)(config.num_messages * 0.999)];
     
     // Print results
     printf("\nResults:\n");
     printf("Avg latency: %.4f ms\n", avg_latency);
     printf("Percentiles: 50th = %d, 99th = %d, 99.9th = %d\n", p50, p99, p999);
 
 cleanup:
     // Clean up
     if (latencies) free(latencies);
     if (message) free(message);
     if (consumer) {
         rd_kafka_consumer_close(consumer);
         rd_kafka_destroy(consumer);
     }
     if (producer) {
         rd_kafka_flush(producer, 10*1000); // Wait for outstanding messages
         rd_kafka_destroy(producer);
     }
     
     return 0;
 }