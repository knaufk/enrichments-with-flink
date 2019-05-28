package com.github.knaufk.enrichments.perevent;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import com.github.knaufk.enrichments.entities.CacheEntry;
import com.github.knaufk.enrichments.entities.EnrichedMeasurements;
import com.github.knaufk.enrichments.entities.SensorMeasurement;
import com.github.knaufk.enrichments.entities.SensorReferenceData;
import com.github.knaufk.enrichments.io.SensorReferenceDataClient;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class SyncEnrichmentFunctionWithCache
		extends RichFlatMapFunction<SensorMeasurement, EnrichedMeasurements> {

	public static final Time CACHE_TIMEOUT = Time.of(1, TimeUnit.MINUTES);

	private transient SensorReferenceDataClient client;
	private transient Map<Long, CacheEntry<SensorReferenceData>> cache;

	@Override
	public void open(final Configuration parameters) throws Exception {
		super.open(parameters);
		client = new SensorReferenceDataClient();
		cache = new HashMap<>();
	}

	@Override
	public void flatMap(
			final SensorMeasurement sensorMeasurement,
			final Collector<EnrichedMeasurements> collector) throws Exception {

		CacheEntry<SensorReferenceData> cacheEntry = cache.get(sensorMeasurement.getSensorId());

		if (cacheEntry != null && cacheEntry.isExpired(CACHE_TIMEOUT)) {
			collector.collect(new EnrichedMeasurements(sensorMeasurement, cacheEntry.getCached()));
		} else {
			SensorReferenceData referenceData = client.getSensorReferenceDataFor(sensorMeasurement.getSensorId());
			cache.put(sensorMeasurement.getSensorId(), new CacheEntry<>(System.currentTimeMillis(), referenceData));
			collector.collect(new EnrichedMeasurements(sensorMeasurement, referenceData));
		}
	}
}
