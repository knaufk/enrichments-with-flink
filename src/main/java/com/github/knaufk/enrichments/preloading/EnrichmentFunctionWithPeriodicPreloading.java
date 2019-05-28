package com.github.knaufk.enrichments.preloading;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import com.github.knaufk.enrichments.entities.EnrichedMeasurements;
import com.github.knaufk.enrichments.entities.SensorMeasurement;
import com.github.knaufk.enrichments.entities.SensorReferenceData;
import com.github.knaufk.enrichments.io.SensorReferenceDataClient;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.TimeUnit;

@Slf4j
public class EnrichmentFunctionWithPeriodicPreloading extends
                                                      KeyedProcessFunction<Long, SensorMeasurement, EnrichedMeasurements> {

	private Map<Long, SensorReferenceData> referenceData;

	@Override
	public void open(final Configuration parameters) throws Exception {
		super.open(parameters);
		referenceData = loadReferenceData();

	}

	@Override
	public void processElement(
			final SensorMeasurement sensorMeasurement,
			final Context context,
			final Collector<EnrichedMeasurements> collector) throws Exception {

			SensorReferenceData sensorReferenceData = referenceData.get(sensorMeasurement.getSensorId());
			collector.collect(new EnrichedMeasurements(sensorMeasurement, sensorReferenceData));

			context.timerService().registerProcessingTimeTimer(forTheNextFullHour());
	}

	@Override
	public void onTimer(
			final long timestamp,
			final OnTimerContext ctx,
			final Collector<EnrichedMeasurements> out) throws Exception {
		referenceData = loadReferenceData();
	}

	private long forTheNextFullHour() {
		long millisPerHour = Time.of(1, TimeUnit.HOURS).toMilliseconds();

		return (System.currentTimeMillis() / millisPerHour) * millisPerHour + millisPerHour;
	}

	private Map<Long, SensorReferenceData> loadReferenceData() {
		log.info("Reloading Reference Data...");
		SensorReferenceDataClient client = new SensorReferenceDataClient();
		return client.getSensorReferenceData();
	}

}
