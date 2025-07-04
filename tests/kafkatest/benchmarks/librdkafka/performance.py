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
from ducktape.services.background_thread import BackgroundThreadService

class LibrdKafkaPerformance(BackgroundThreadService):
    """Base class for librdkafka performance services"""

    def __init__(self, context=None, num_nodes=0, root="/mnt/rdkafka_performance", stop_timeout_sec=30):
        """
        Initialize the RdKafka Performance Service
        
        Args:
            context: Test context
            num_nodes: Number of nodes to use
            root: Root directory for persistent files
            stop_timeout_sec: Timeout for stopping the service
        """
        super(LibrdKafkaPerformance, self).__init__(context, num_nodes)
        
        self.persistent_root = root
        self.stop_timeout_sec = stop_timeout_sec
        
        # Initialize results
        self.results = [None] * num_nodes
        self.stats = [[] for x in range(num_nodes)]

    def process_name(self):
        """
        Returns the name of the process which this service creates.
        This is used to identify and stop the process.
        Subclasses should override this method.
        """
        return ""

    def pids(self, node, process_grep_pattern=None):
        """
        Return process IDs for processes on the given node.
        
        Args:
            node: The node to check
            process_grep_pattern: Optional pattern to grep for (defaults to process_name())
            
        Returns:
            List of process IDs
        """
        grep_pattern = process_grep_pattern or self.process_name()
        try:
            cmd = f"ps ax | grep -i {grep_pattern} | grep -v grep | awk '{{print $1}}'"
            pid_arr = [pid for pid in node.account.ssh_capture(cmd, allow_fail=True)]
            return pid_arr
        except:
            return []

    def alive(self, node, process_grep_pattern=None):
        """
        Return True if the process is alive on the given node.
        
        Args:
            node: The node to check
            process_grep_pattern: Optional pattern to grep for (defaults to process_name())
            
        Returns:
            True if the process is alive, False otherwise
        """
        return len(self.pids(node, process_grep_pattern)) > 0

    def stop_node(self, node):
        """
        Stop the service on the given node.
        
        Args:
            node: The node to stop the service on
        """
        if not self.process_name():
            self.logger.warning("No process name specified, cannot stop node")
            return
            
        self.logger.info("Stopping %s process on %s", self.process_name(), node.account.hostname)
        node.account.kill_process(self.process_name(), allow_fail=True)
        
        stopped = self.wait_node(node, timeout_sec=self.stop_timeout_sec)
        assert stopped, f"Node {node.account.hostname}: did not stop within the specified timeout of {self.stop_timeout_sec} seconds"

    def clean_node(self, node):
        """
        Clean up resources on the given node.
        
        Args:
            node: The node to clean
        """
        if not self.process_name():
            self.logger.warning("No process name specified, cannot clean node")
            return
            
        self.logger.info("Cleaning %s on %s", self.process_name(), node.account.hostname)
        node.account.kill_process(self.process_name(), clean_shutdown=False, allow_fail=True)
        node.account.ssh(f"rm -rf {self.persistent_root}", allow_fail=True)

    def parse_json_file(self, node, file_path):
        """
        Parse a JSON file on the given node.
        
        Args:
            node: The node with the file
            file_path: Path to the JSON file
            
        Returns:
            Parsed JSON object
        """
        contents = node.account.ssh_capture(f"cat {file_path}")
        json_str = "".join(contents)
        return json.loads(json_str)