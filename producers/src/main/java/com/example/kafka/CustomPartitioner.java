package com.example.kafka;

import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.internals.StickyPartitionCache;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomPartitioner implements Partitioner {

	public static final Logger logger = LoggerFactory.getLogger(CustomPartitioner.class.getName());
	private final StickyPartitionCache stickyPartitionCache = new StickyPartitionCache();
	public String specialKeyName;

	@Override
	public void configure(Map<String, ?> configs) {
		specialKeyName= configs.get("custom.specialKey").toString();
	}

	@Override
	public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {

		List<PartitionInfo> partitionInfoList = cluster.partitionsForTopic(topic);
		int numPartitions = partitionInfoList.size();
		int numSpecialPartitions = (int)(numPartitions * 0.5); // 2
		int partitionIndex = 0;

		if (keyBytes == null) {
			// return stickyPartitionCache.partition(topic, cluster);
			throw new InvalidRecordException("key should not be null");
		}

		if (((String)key).equals(specialKeyName)) {
			partitionIndex = Utils.toPositive(Utils.murmur2(valueBytes)) % numSpecialPartitions; // keyBytes가 들어가야하지만 P001로 고정이니 value를 넣어 파티션으로 분산되는 것을 확인
		}else {
			partitionIndex= Utils.toPositive(Utils.murmur2(keyBytes)) % (numPartitions - numSpecialPartitions) + numSpecialPartitions;
		}
		logger.info("key:{} is sent to partition:{}", key.toString(), partitionIndex);

		return partitionIndex;
	}

	@Override
	public void close() {

	}
}
