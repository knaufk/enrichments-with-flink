package com.github.knaufk.enrichments.entities;

import org.apache.flink.api.common.time.Time;

import lombok.Data;

@Data
public class CacheEntry<T> {

	private final long timestamp;
	private final T cached;

	public boolean isExpired(Time timeout) {
		return System.currentTimeMillis() - timeout.toMilliseconds() < timestamp;
	}

}
