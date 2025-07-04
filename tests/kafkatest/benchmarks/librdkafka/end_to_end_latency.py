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
from kafkatest.services.performance import PerformanceService
from kafkatest.services.security.security_config import SecurityConfig
from kafkatest.benchmarks.librdkafka.setup_utils import setup_librdkafka, setup_ssl_certificates
from kafkatest.version import get_version, V_3_4_0, DEV_BRANCH

class LibrdKafkaEndToEndLatency(PerformanceService):
    MESSAGE_BYTES = 21  # 0.8.X messages are fixed at 21 bytes, so we'll match that for other versions
    """Service for measuring end-to-end latency using librdkafka"""

    def __init__(self, context, num_nodes, kafka, topic, num_records, version=DEV_BRANCH, settings=None):
        """
        Initialize the RdKafka End-to-End Latency Service
        
        Args:
            context: Test context
            num_nodes: Number of nodes to use
            kafka: Kafka service
            topic: Topic to use for the benchmark
            num_records: Number of records to send
            message_size: Size of each message in bytes
            acks: Producer acks setting ("1" or "all")
            settings: Additional settings for rdkafka
        """
        root = "/mnt/rdkafka_e2e_latency"
        super(LibrdKafkaEndToEndLatency, self).__init__(context, num_nodes, root=root)
        
        self.kafka = kafka
        self.topic = topic
        self.num_records = num_records
        self.message_size = LibrdKafkaEndToEndLatency.MESSAGE_BYTES
        self.settings = settings or {}
        self.security_config = kafka.security_config.client_config() if hasattr(kafka, 'security_config') else None
        
        # Set up log files
        self.stdout_path = os.path.join(root, "e2e_latency.stdout")
        self.stderr_path = os.path.join(root, "e2e_latency.stderr")
        self.config_file = os.path.join(root, "e2e_latency.config")
        self.c_executable = os.path.join("/mnt/rdkafka", "e2e_latency")

        self.logs = {
            "e2e_latency_stdout": {
                "path": self.stdout_path,
                "collect_default": True
            },
            "e2e_latency_stderr": {
                "path": self.stderr_path,
                "collect_default": True
            }
        }

        # Log security protocol if available
        if self.security_config and hasattr(self.security_config, 'security_protocol'):
            self.logger.info(f"Using security protocol: {self.security_config.security_protocol}")

        # Flag to track if librdkafka has been set up
        self.librdkafka_setup = False

        for node in self.nodes:
            node.version = version

    def process_name(self):
        """Override process_name to return the service name"""
        return "rdkafka_e2e_latency"

    def get_bootstrap_servers(self):
        """Return the bootstrap servers string for the Kafka cluster."""
        return self.kafka.bootstrap_servers(self.security_config.security_protocol)

    def create_config_file(self, node):
        """Create a configuration file for rdkafka on the given node."""
        # Ensure the directory exists
        node.account.ssh(f"mkdir -p {os.path.dirname(self.config_file)}")
        
        config_lines = []
        for key, value in self.settings.items():
            config_lines.append(f"{key}={value}")
        
        # Write the config file
        node.account.create_file(self.config_file, "\n".join(config_lines))

    def start_cmd(self, node):
        """Build the command to run the end-to-end latency test"""
        bootstrap_servers = self.get_bootstrap_servers()
        
        cmd = f"{self.c_executable} "
        cmd += f"\"{bootstrap_servers}\" "  # brokers
        cmd += f"\"{self.topic}\" "         # topic
        cmd += f"{self.num_records} "       # num_messages
        cmd += f"{self.message_size} "      # message_size        
        cmd += f"\"{self.config_file}\" "   # config_file
        
        if self.security_config.security_protocol in (SecurityConfig.SASL_SSL, SecurityConfig.SASL_PLAINTEXT):
            cmd = (
                "export KRB5_CONFIG=/mnt/security/krb5.conf; "
                "export KRB5_KTNAME=/mnt/security/keytab; "
                "kinit -kt /mnt/security/keytab client@EXAMPLE.COM && "
                f"{cmd}"
            )

        # Redirect output to both console and files using tee
        cmd += " 2>> %(stderr)s | tee -a %(stdout)s" % {
            'stdout': self.stdout_path,
            'stderr': self.stderr_path
        }
                
        self.security_config.setup_node(node)
        # Configure security if available
        if self.security_config.security_protocol in (SecurityConfig.SSL, SecurityConfig.SASL_SSL):
            # Set up SSL certificates if needed            
            setup_ssl_certificates(node)

        return cmd

    def _worker(self, idx, node):
        """Worker method that runs the end-to-end latency test on a node"""
        try:
            # Prepare environment
            node.account.ssh(f"mkdir -p {self.root}")
            
            # Create config file
            self.create_config_file(node)
            
            # Run the test
            cmd = self.start_cmd(node)
            self.logger.debug("End-to-end latency command: %s", cmd)
            
            # Execute and parse results from stdout
            results = {}
            for line in node.account.ssh_capture(cmd):
                if line.startswith("Avg latency:"):
                    results['latency_avg_ms'] = float(line.split()[2])
                elif line.startswith("Percentiles:"):
                    parts = line.split()
                    results['latency_50th_ms'] = int(parts[3][:-1])
                    results['latency_99th_ms'] = int(parts[6][:-1])
                    results['latency_999th_ms'] = int(parts[9])
            
            self.results[idx-1] = results
            
        except Exception as e:
            self.logger.error(f"Error in worker {idx}: {e}")
            raise

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
        super(LibrdKafkaEndToEndLatency, self).run()