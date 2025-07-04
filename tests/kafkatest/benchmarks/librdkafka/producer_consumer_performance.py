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

import os
import json
import re
import time
from ducktape.utils.util import wait_until
from kafkatest.services.security.security_config import SecurityConfig
from kafkatest.benchmarks.librdkafka.performance import LibrdKafkaPerformance
from kafkatest.benchmarks.librdkafka.setup_utils import setup_librdkafka, setup_ssl_certificates
from kafkatest.version import get_version, V_3_4_0, DEV_BRANCH

class LibrdKafkaProducerConsumerPerformance (LibrdKafkaPerformance):
    """Service for measuring producer/consumer performance using librdkafka"""

    def __init__(self, context, num_nodes, kafka, topic, num_records, record_size=100, throughput=-1, 
                 mode="producer", version=DEV_BRANCH, settings=None, intermediate_stats=False):
        """
        Initialize the RdKafka Producer/Consumer Performance Service
        
        Args:
            context: Test context
            num_nodes: Number of nodes to use
            kafka: Kafka service
            topic: Topic to use for the benchmark
            num_records: Number of records to produce/consume
            record_size: Size of each record in bytes
            throughput: Target throughput in records/sec (-1 for unlimited)
            mode: "producer" or "consumer"
            settings: Additional settings for rdkafka
            intermediate_stats: Whether to collect intermediate stats
        """
        # Initialize base class with basic parameters
        service_name = "rdkafka_performance"
        root = f"/mnt/{service_name}"
        super(LibrdKafkaProducerConsumerPerformance, self).__init__(
            context=context, 
            num_nodes=num_nodes,
            root=root,
            stop_timeout_sec=30
        )
        
        # Store additional parameters
        self.kafka = kafka
        self.topic = topic
        self.settings = settings or {}
        self.service_name = service_name
        self.num_records = num_records
        self.record_size = record_size
        self.throughput = throughput
        self.mode = mode
        self.intermediate_stats = intermediate_stats        
        
        # Get security configuration from Kafka
        self.security_config = kafka.security_config.client_config() if hasattr(kafka, 'security_config') else None
        
        # Set up config file path
        self.config_file = os.path.join(self.persistent_root, f"{service_name}.config")
        
        # Set up log files
        self.logs = {
            "rdkafka_perf_stdout": {
                "path": os.path.join(self.persistent_root, "rdkafka_performance.stdout"),
                "collect_default": True
            },
            "rdkafka_perf_stderr": {
                "path": os.path.join(self.persistent_root, "rdkafka_performance.stderr"),
                "collect_default": True
            },
            "rdkafka_stats": {
                "path": os.path.join(self.persistent_root, "rdkafka_stats.json"),
                "collect_default": True
            }
        }
        
        # Define paths for files
        self.stdout_path = self.logs["rdkafka_perf_stdout"]["path"]
        self.stderr_path = self.logs["rdkafka_perf_stderr"]["path"]
        self.stats_path = self.logs["rdkafka_stats"]["path"]
        
        # Set client ID
        self.client_id = f"{self.service_name}_{self.mode}"
        
        # Update settings with client ID and statistics configuration
        self.settings.update({
            "client.id": self.client_id,
            "statistics.interval.ms": "1000"
        })
        
        # Log security protocol if available
        if self.security_config and hasattr(self.security_config, 'security_protocol'):
            self.logger.info(f"Using security protocol: {self.security_config.security_protocol}")

        # Flag to track if librdkafka has been set up
        self.librdkafka_setup = False

        for node in self.nodes:
            node.version = version

    def process_name(self):
        """Override process_name to return the service name"""
        return "rdkafka_performance"

    def get_bootstrap_servers(self):
        """Return the bootstrap servers string for the Kafka cluster."""
        return self.kafka.bootstrap_servers(self.kafka.security_config.security_protocol)

    def create_config_file(self, node):
        """Create a configuration file for rdkafka on the given node."""
        # Ensure the directory exists
        node.account.ssh(f"mkdir -p {os.path.dirname(self.config_file)}")
        
        config_lines = []
        for key, value in self.settings.items():
            config_lines.append(f"{key}={value}")
        
        # Write the config file
        node.account.create_file(self.config_file, "\n".join(config_lines))
    
    def run(self):
        """
        Run the performance test on all nodes
        """
        # Set up librdkafka on all nodes before starting workers
        if not self.librdkafka_setup:
            
            self.logger.info(f"Setting up librdkafka on {len(self.nodes)} nodes")
            setup_success = setup_librdkafka(nodes=self.nodes)
            
            if not setup_success:
                self.logger.warning("Failed to set up librdkafka on some nodes. Tests may fail.")
            
            self.librdkafka_setup = True
        
        # Now run the workers
        super(LibrdKafkaProducerConsumerPerformance, self).run()
    
    def start_cmd(self, node):
        """
        Build the command to run rdkafka_performance
        
        Args:
            node: The node to run the command on
            
        Returns:
            The command string
        """
        bootstrap_servers = self.get_bootstrap_servers()
        
        # Command to run rdkafka_performance
        cmd = "rdkafka_performance "
        
        # Add mode-specific parameters
        if self.mode == "producer":
            cmd += "-P "
            cmd += f"-s {self.record_size} "
        else:  # consumer
            cmd += "-C "
            if "group.id" in self.settings:
                group_id = self.settings["group.id"]
                cmd += f"-G {group_id} "
        
        # Add common parameters
        cmd += f"-t {self.topic} "
        cmd += f"-b {bootstrap_servers} "        
        cmd += f"-c {self.num_records} "
        
        # Add throughput limit if specified
        # if self.throughput > 0:
        cmd += f"-r {self.throughput} "
        
        # Add config file
        cmd += f"-X file={self.config_file} "
        
        # Add intermediate stats if requested
        # if self.intermediate_stats:
        cmd += "-i 5000 "  # Print stats every 5000ms

         # Add compression parameter if specified and not "none"
        if self.mode == "producer" and "compression.type" in self.settings and self.settings["compression.type"].lower() != "none":
            compression_type = self.settings["compression.type"].lower()
            # Validate compression type (librdkafka only supports none, gzip, snappy)
            if compression_type in ["gzip", "snappy"]:
                cmd += f"-z {compression_type} "            

        # Add statistics capture using -Y option
        # This will append JSON statistics to the stats file
        cmd += f"-T 5000 -Y 'cat >> {self.stats_path}' "
    
        if self.security_config.security_protocol in (SecurityConfig.SASL_SSL, SecurityConfig.SASL_PLAINTEXT):
            cmd = (
                "export KRB5_CONFIG=/mnt/security/krb5.conf; "
                "export KRB5_KTNAME=/mnt/security/keytab; "
                "kinit -kt /mnt/security/keytab client@EXAMPLE.COM && "
                f"{cmd}"
            )

        # Redirect output
        cmd += f"> {self.stdout_path} 2> {self.stderr_path}"
        
        self.security_config.setup_node(node)
        # Configure security if available
        if self.security_config.security_protocol in (SecurityConfig.SSL, SecurityConfig.SASL_SSL):
            # Set up SSL certificates if needed            
            setup_ssl_certificates(node)
            
        return cmd        
    
    def _worker(self, idx, node):
        """
        Worker method that runs the performance test on a node.
        This method is called by BackgroundThreadService for each node.
        
        Args:
            idx: Worker index
            node: The node to run on
        """
        try:
            # Prepare the environment
            node.account.ssh(f"mkdir -p {self.persistent_root}")
            
            # Create configuration file
            self.create_config_file(node)
            
            # Ensure that the statistics directory exists
            node.account.ssh(f"mkdir -p {os.path.dirname(self.stats_path)}")
            node.account.ssh(f"touch {self.stats_path}")
            
            # Run the performance test
            cmd = self.start_cmd(node)
            self.logger.info(f"Starting performance test on node {idx} with command: {cmd}")
            
            # Record start time
            start_time = time.time()
            
            # Start the process
            node.account.ssh(f"bash -lc \"{cmd}\"")
            
            # Record end time
            elapsed_time = time.time() - start_time
            self.logger.info(f"RdKafka performance process on node {idx} ran for {elapsed_time:.2f} seconds")
        
            # Analyse results            
            stdout_content = node.account.ssh_capture(f"cat {self.stdout_path}")            
            stats = []
            for line in stdout_content:
                try:        
                    parsed_stats = self.parse_stats(line)                    
                    if parsed_stats:
                        stats.append(parsed_stats)
                except Exception as e:
                    self.logger.warning(f"Ignoring line during intermediate stats parsing: {line.strip()} (Error: {e})")
                    pass
            try:                
                # Store parsed stats                
                self.results[idx - 1] = stats[-1]                

                # Parse last JSON for latency info
                json_stats = self.get_last_json_stats(node)
                if json_stats :
                    latency_metrics = self.parse_latency_metrics(json_stats)
                    self.results[idx - 1].update(latency_metrics)

                if self.intermediate_stats:
                    # interval_stats = self.convert_to_interval_stats(stats)
                    # self.stats[idx - 1] = interval_stats
                    self.stats[idx - 1].append(self.results[idx - 1])                    

                self.logger.info(f"Stadistics for node {idx}: {self.results[idx - 1]}")
                if self.intermediate_stats:
                    self.logger.info(f"Complete Stadistics for node {idx}: {self.stats[idx - 1]}")
            except:
                raise Exception("Unable to parse aggregate performance statistics on node %d: %s" % (idx, self.results[idx - 1]))            

        except Exception as e:
            self.logger.error(f"Error in worker: {e}")
            raise

    def convert_to_interval_stats(self, stats):
        """
        Convert a list of cumulative statistics to interval-based statistics.
        
        Args:
            stats: List of dictionaries containing cumulative statistics
            
        Returns:
            List of dictionaries with interval-based statistics
        """
        if not stats:
            return []
        
        interval_stats = []
        for i, current_stat in enumerate(stats):
            # For the first entry, use the cumulative values as the interval stats
            if i == 0:
                stat_entry = current_stat.copy()
                # Recalculate rates for consistency, though they may match the original
                if stat_entry['duration_ms'] > 0:
                    if self.mode == "producer":
                        stat_entry['records_per_sec'] = (stat_entry['records'] * 1000.0) / stat_entry['duration_ms']
                        stat_entry['mbps'] = self.record_size * stat_entry['records_per_sec'] / 1024 / 1024
                        # stat_entry['ms_per_batch'] = 1000.0 / stat_entry['records_per_sec'] if stat_entry['records_per_sec'] > 0 else 0
                    else:  # consumer
                        stat_entry['records_per_sec'] = (stat_entry['records'] * 1000.0) / stat_entry['duration_ms']
                        stat_entry['mbps'] = (stat_entry['bytes_consumed'] * 1000.0) / stat_entry['duration_ms'] / 1000 / 1000 # Convert bytes to MB/s = 1mb = 1^6 b
                else:
                    stat_entry['records_per_sec'] = 0
                    stat_entry['mbps'] = 0
                    if self.mode == "producer":
                        stat_entry['ms_per_batch'] = 0
                interval_stats.append(stat_entry)
                continue
            
            # Get the previous cumulative stats
            prev_stat = stats[i - 1]
            
            # Compute interval-based stats by subtracting previous cumulative values
            stat_entry = {}
            if self.mode == "producer":
                stat_entry = {
                    'records_produced': current_stat['records_produced'] - prev_stat['records_produced'],  # Incremental records produced
                    'records': current_stat['records'] - prev_stat['records'],  # Incremental records delivered
                    'bytes_produced': current_stat['bytes_produced'] - prev_stat['bytes_produced'],
                    'delivery_failures': current_stat['delivery_failures'] - prev_stat['delivery_failures'],
                    'produce_failures': current_stat['produce_failures'] - prev_stat['produce_failures'],
                    'queue_remaining': current_stat['queue_remaining'],  # Current queue state, not a delta
                    'duration_ms': current_stat['duration_ms'] - prev_stat['duration_ms'],
                    # 'latency_avg_ms': current_stat['latency_avg_ms'],
                    # 'latency_max_ms': current_stat['latency_max_ms'],
                    # 'latency_50th_ms': current_stat['latency_50th_ms'],
                    # 'latency_95th_ms': current_stat['latency_95th_ms'],
                    # 'latency_99th_ms': current_stat['latency_99th_ms'],
                    # 'latency_999th_ms': current_stat['latency_999th_ms']
                }
                # Compute records per second and MB/s for the interval
                if stat_entry['duration_ms'] > 0:
                    stat_entry['records_per_sec'] = (stat_entry['records'] * 1000.0) / stat_entry['duration_ms']
                    stat_entry['mbps'] = self.record_size * stat_entry['records_per_sec'] / 1000 / 1000  # Convert bytes to MB/s = 1mb = 1^6 b
                    # stat_entry['ms_per_batch'] = 1000.0 / stat_entry['records_per_sec'] if stat_entry['records_per_sec'] > 0 else 0
                else:
                    stat_entry['records_per_sec'] = 0
                    stat_entry['mbps'] = 0
                    # stat_entry['ms_per_batch'] = 0
            else:  # consumer
                stat_entry = {
                    'records': current_stat['records'] - prev_stat['records'],
                    'bytes_consumed': current_stat['bytes_consumed'] - prev_stat['bytes_consumed'],
                    'duration_ms': current_stat['duration_ms'] - prev_stat['duration_ms'],
                    # 'latency_avg_ms': current_stat['latency_avg_ms'],
                    # 'latency_max_ms': current_stat['latency_max_ms'],
                    # 'latency_50th_ms': current_stat['latency_50th_ms'],
                    # 'latency_95th_ms': current_stat['latency_95th_ms'],
                    # 'latency_99th_ms': current_stat['latency_99th_ms'],
                    # 'latency_999th_ms': current_stat['latency_999th_ms']
                }
                # Compute records per second and MB/s for the interval
                if stat_entry['duration_ms'] > 0:
                    stat_entry['records_per_sec'] = (stat_entry['records'] * 1000.0) / stat_entry['duration_ms']
                    stat_entry['mbps'] = self.record_size * stat_entry['records_per_sec'] / 1000 / 1000  # Convert bytes to MB/s = 1mb = 1^6 b
                else:
                    stat_entry['records_per_sec'] = 0
                    stat_entry['mbps'] = 0
            
            interval_stats.append(stat_entry)
        
        return interval_stats
    
    def parse_stats(self, line):
        """
        Parse a single line of stdout to extract statistics
        
        Args:
            line: A line from stdout containing statistics
            backpressure_info: Dictionary with current backpressure information
            
        Returns:
            Dictionary with statistics or None if the line doesn't contain statistics
        """
        
        # Check for statistics information
        if "messages produced" in line:
            # Regex pattern to extract information
            stats_pattern = (
                r"% (\d+) messages produced \((\d+) bytes\), (\d+) delivered "
                r"\(offset \d+, (\d+) failed\) in (\d+)ms: (\d+) msgs/s and "
                r"([\d\.]+) MB/s, (\d+) produce failures, (\d+) in queue"
           )
            
            match = re.search(stats_pattern, line)
            if match:
                # Create statistics dictionary for this line
                stat_entry = {
                    'records_produced': int(match.group(1)),
                    'bytes_produced': int(match.group(2)),
                    'records': int(match.group(3)),
                    'delivery_failures': int(match.group(4)),
                    'duration_ms': int(match.group(5)),
                    'records_per_sec': float(match.group(6)),
                    'mbps': float(match.group(7)),
                    'produce_failures': int(match.group(8)),
                    'queue_remaining': int(match.group(9)),                    
                }
                
                return stat_entry
        # Check for consumer statistics information
        elif "messages (" in line and "consumed in" in line:
            # Regex pattern to extract consumer information
            consumer_pattern = (
                r"% (\d+) messages \((\d+) bytes\) consumed in (\d+)ms: (\d+) msgs/s \(([\d\.]+) MB/s\)"
            )
        
            match = re.search(consumer_pattern, line)
            if match:
                # Create statistics dictionary for this line
                stat_entry = {
                    'records': int(match.group(1)),
                    'bytes_consumed': int(match.group(2)),
                    'duration_ms': int(match.group(3)),
                    'records_per_sec': float(match.group(4)),
                    'mbps': float(match.group(5)),                    
                }
                
                return stat_entry
        return None  # This line doesn't contain statistics

    def parse_latency_metrics(self, json_stats):
        """
        Extract latency metrics from JSON stats using only documented fields
        and aggregate across all brokers
        
        Args:
            json_stats: The JSON stats object
            
        Returns:
            Dictionary with latency metrics
        """
        latency_metrics = {
            'latency_avg_ms': 0,
            'latency_max_ms': 0,
            'latency_50th_ms': 0,
            'latency_95th_ms': 0,
            'latency_99th_ms': 0,
            'latency_999th_ms': 0
        }
        
        try:
            # For producer, look in int_latency (internal producer queue latency)
            if self.mode == "producer" and "brokers" in json_stats:
                int_latencies = []
                total_cnt = 0
                
                # Collect all int_latency metrics from all brokers
                for broker_id, broker_data in json_stats["brokers"].items():
                    if isinstance(broker_data, dict) and "int_latency" in broker_data and broker_data["int_latency"].get("cnt", 0) > 0:
                        latency = broker_data["int_latency"]
                        cnt = int(latency.get("cnt", 0))
                        if cnt > 0:
                            int_latencies.append((latency, cnt))
                            total_cnt += cnt
                
                # If we found latencies, calculate weighted averages
                if int_latencies and total_cnt > 0:
                    # Weighted average
                    latency_metrics['latency_avg_ms'] = sum(float(l.get("avg", 0)) * cnt for l, cnt in int_latencies) / total_cnt / 1000.0
                    
                    # Global maximum
                    latency_metrics['latency_max_ms'] = max(float(l.get("max", 0)) for l, _ in int_latencies) / 1000.0
                    
                    # Weighted percentiles
                    latency_metrics['latency_50th_ms'] = sum(float(l.get("p50", 0)) * cnt for l, cnt in int_latencies) / total_cnt / 1000.0
                    latency_metrics['latency_95th_ms'] = sum(float(l.get("p95", 0)) * cnt for l, cnt in int_latencies) / total_cnt / 1000.0
                    latency_metrics['latency_99th_ms'] = sum(float(l.get("p99", 0)) * cnt for l, cnt in int_latencies) / total_cnt / 1000.0
                    latency_metrics['latency_999th_ms'] = sum(float(l.get("p99_99", 0)) * cnt for l, cnt in int_latencies) / total_cnt / 1000.0
                    
                    return latency_metrics
            
            # For both modes (producer and consumer), look in rtt
            if "brokers" in json_stats:
                rtt_latencies = []
                total_cnt = 0
                
                # Collect all rtt latencies from all brokers
                for broker_id, broker_data in json_stats["brokers"].items():
                    if isinstance(broker_data, dict) and "rtt" in broker_data and broker_data["rtt"].get("cnt", 0) > 0:
                        latency = broker_data["rtt"]
                        cnt = int(latency.get("cnt", 0))
                        if cnt > 0:
                            rtt_latencies.append((latency, cnt))
                            total_cnt += cnt
                
                # If we found rtt latencies, calculate weighted averages
                if rtt_latencies and total_cnt > 0:
                    # Weighted average
                    latency_metrics['latency_avg_ms'] = sum(float(l.get("avg", 0)) * cnt for l, cnt in rtt_latencies) / total_cnt / 1000.0
                    
                    # Global maximum
                    latency_metrics['latency_max_ms'] = max(float(l.get("max", 0)) for l, _ in rtt_latencies) / 1000.0
                    
                    # Weighted percentiles
                    latency_metrics['latency_50th_ms'] = sum(float(l.get("p50", 0)) * cnt for l, cnt in rtt_latencies) / total_cnt / 1000.0
                    latency_metrics['latency_95th_ms'] = sum(float(l.get("p95", 0)) * cnt for l, cnt in rtt_latencies) / total_cnt / 1000.0
                    latency_metrics['latency_99th_ms'] = sum(float(l.get("p99", 0)) * cnt for l, cnt in rtt_latencies) / total_cnt / 1000.0
                    latency_metrics['latency_999th_ms'] = sum(float(l.get("p99_99", 0)) * cnt for l, cnt in rtt_latencies) / total_cnt / 1000.0
                    
                    return latency_metrics
            
            # If we get here, we didn't find any latency metrics
            self.logger.warning("No latency metrics found in JSON stats")
            return latency_metrics
            
        except Exception as e:
            self.logger.error(f"Error extracting latency metrics: {str(e)}")
            return latency_metrics

    def get_last_json_stats(self, node):
        """
        Get the last JSON stats entry from the stats file
        
        Args:
            node: The node to get stats from
            
        Returns:
            The last JSON stats object or None if not found
        """
        # Check if the stats file exists
        if not node.account.exists(self.stats_path):
            self.logger.warning(f"Stats file {self.stats_path} does not exist")
            return None
        
        # Read the last line of the JSON file
        last_line = node.account.ssh_capture(f"tail -n 1 {self.stats_path}")
        
        # Process the last JSON line
        for line in last_line:  # Should be just one line
            try:
                # Parse the JSON
                json_stats = json.loads(line)
                return json_stats
            except json.JSONDecodeError as e:
                self.logger.warning(f"Failed to parse last JSON line: {e}")
            except Exception as e:
                self.logger.warning(f"Error processing last JSON stats: {e}")
        
        return None



    