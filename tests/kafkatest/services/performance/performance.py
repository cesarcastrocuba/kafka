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

from ducktape.services.background_thread import BackgroundThreadService
from kafkatest.directory_layout.kafka_path import KafkaPathResolverMixin


class PerformanceService(KafkaPathResolverMixin, BackgroundThreadService):

    def __init__(self, context=None, num_nodes=0, root="/mnt/*", stop_timeout_sec=30):
        super(PerformanceService, self).__init__(context, num_nodes)
        self.results = [None] * self.num_nodes
        self.stats = [[] for x in range(self.num_nodes)]
        self.stop_timeout_sec = stop_timeout_sec
        self.root = root

    def java_class_name(self):
        """
        Returns the name of the Java class which this service creates.  Subclasses should override
        this method, so that we know the name of the java process to stop.  If it is not
        overridden, we will kill all java processes in PerformanceService#stop_node (for backwards
        compatibility.)
        """
        return ""

    def stop_node(self, node):
        node.account.kill_java_processes(self.java_class_name(), clean_shutdown=True, allow_fail=True)

        stopped = self.wait_node(node, timeout_sec=self.stop_timeout_sec)
        assert stopped, "Node %s: did not stop within the specified timeout of %s seconds" % \
                        (str(node.account), str(self.stop_timeout_sec))

    def clean_node(self, node):
        node.account.kill_java_processes(self.java_class_name(), clean_shutdown=False, allow_fail=True)
        node.account.ssh("rm -rf -- %s" % self.root, allow_fail=False)


def throughput(records_per_sec, mb_per_sec, records=None, duration_ms=None, execution_id=None):
    """Helper method to ensure uniform representation of throughput data"""
    result = {
        "records_per_sec": records_per_sec,
        "mb_per_sec": mb_per_sec
    }

    if records is not None:
        result["records"] = records
    if duration_ms is not None:
        result["duration_ms"] = duration_ms
    if execution_id is not None:
        result["execution_id"] = execution_id

    return result

def latency(latency_50th_ms, latency_99th_ms, latency_999th_ms, execution_id=None):
    
    """Helper method to ensure uniform representation of latency data"""
    result = {
        "latency_50th_ms": latency_50th_ms,
        "latency_99th_ms": latency_99th_ms,
        "latency_999th_ms": latency_999th_ms
    }
    if execution_id is not None:
        result["execution_id"] = execution_id
    return result


def compute_aggregate_throughput(perf, execution_id=None):
    """Helper method for computing throughput after running a performance service."""
    aggregate_rate = sum([r['records_per_sec'] for r in perf.results])
    aggregate_mbps = sum([r['mbps'] for r in perf.results])
    aggregate_records = sum([r['records'] for r in perf.results])

    durations = [r.get('duration_ms') for r in perf.results if 'duration_ms' in r]
    if len(durations) == len(perf.results):
        duration_ms = max(durations)
    else:
        duration_ms = (aggregate_records / aggregate_rate * 1000) if aggregate_rate > 0 else 0    
    
    return throughput(aggregate_rate, aggregate_mbps, aggregate_records, duration_ms, execution_id)
