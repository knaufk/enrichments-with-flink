package com.github.knaufk.enrichments.preloading;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import com.github.knaufk.enrichments.entities.EnrichedMeasurements;
import com.github.knaufk.enrichments.entities.SensorMeasurement;
import com.github.knaufk.enrichments.entities.SensorReferenceData;
import com.github.knaufk.enrichments.io.SensorReferenceDataClient;

import java.util.Map;

public class EnrichmentFunctionWithPreloading extends RichFlatMapFunction<SensorMeasurement, EnrichedMeasurements> {

	private Map<Long, SensorReferenceData> referenceData;

	@Override
	public void open(final Configuration parameters) throws Exception {
		super.open(parameters);
		referenceData = loadReferenceData();
	}


	@Override
	public void flatMap(
			final SensorMeasurement sensorMeasurement,
			final Collector<EnrichedMeasurements> collector) throws Exception {
		SensorReferenceData sensorReferenceData = referenceData.get(sensorMeasurement.getSensorId());
		collector.collect(new EnrichedMeasurements(sensorMeasurement, sensorReferenceData));
	}

	private Map<Long, SensorReferenceData> loadReferenceData() {
		SensorReferenceDataClient client = new SensorReferenceDataClient();
		return client.getSensorReferenceData();
	}

}
