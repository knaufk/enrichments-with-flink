package com.github.knaufk.enrichments.perevent;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import com.github.knaufk.enrichments.entities.EnrichedMeasurements;
import com.github.knaufk.enrichments.entities.SensorMeasurement;
import com.github.knaufk.enrichments.entities.SensorReferenceData;
import com.github.knaufk.enrichments.io.SensorReferenceDataClient;

import java.util.Collections;
import java.util.function.Consumer;

public class AsyncEnrichmentFunction extends RichAsyncFunction<SensorMeasurement, EnrichedMeasurements> {

	private SensorReferenceDataClient client;

	@Override
	public void open(final Configuration parameters) throws Exception {
		super.open(parameters);
		client = new SensorReferenceDataClient();
	}

	@Override
	public void asyncInvoke(
			final SensorMeasurement sensorMeasurement,
			final ResultFuture<EnrichedMeasurements> resultFuture) throws Exception {

		client.asyncGetSensorReferenceDataFor(
				sensorMeasurement.getSensorId(),
				new Consumer<SensorReferenceData>() {
					@Override
					public void accept(final SensorReferenceData sensorReferenceData) {
						resultFuture.complete(Collections.singletonList(new EnrichedMeasurements(
								sensorMeasurement,
								sensorReferenceData)));
					}
				});

	}
}
