package com.github.knaufk.enrichments.perevent;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import com.github.knaufk.enrichments.entities.EnrichedMeasurements;
import com.github.knaufk.enrichments.entities.SensorMeasurement;
import com.github.knaufk.enrichments.entities.SensorReferenceData;
import com.github.knaufk.enrichments.io.SensorReferenceDataClient;

public class SyncEnrichmentFunction extends RichFlatMapFunction<SensorMeasurement, EnrichedMeasurements> {

	private SensorReferenceDataClient client;

	@Override
	public void open(final Configuration parameters) throws Exception {
		super.open(parameters);
		client = new SensorReferenceDataClient();
	}

	@Override
	public void flatMap(
			final SensorMeasurement sensorMeasurement,
			final Collector<EnrichedMeasurements> collector) throws Exception {

		SensorReferenceData referenceData = client.getSensorReferenceDataFor(sensorMeasurement.getSensorId());

		collector.collect(new EnrichedMeasurements(sensorMeasurement, referenceData));
	}
}
