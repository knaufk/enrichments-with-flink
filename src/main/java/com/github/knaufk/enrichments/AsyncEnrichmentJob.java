package com.github.knaufk.enrichments;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.github.knaufk.enrichments.entities.EnrichedMeasurements;
import com.github.knaufk.enrichments.entities.SensorMeasurement;
import com.github.knaufk.enrichments.perevent.AsyncEnrichmentFunction;
import com.github.knaufk.enrichments.source.SensorMeasurementSource;

import java.util.concurrent.TimeUnit;

public class AsyncEnrichmentJob {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.enableCheckpointing(5000);

		DataStream<SensorMeasurement> measurements = env.addSource(new SensorMeasurementSource(
				100_000));

		DataStream<EnrichedMeasurements> enrichedMeasurements = AsyncDataStream.unorderedWait(
				measurements,
				new AsyncEnrichmentFunction(), 50, TimeUnit.MILLISECONDS);

		enrichedMeasurements.print();

		env.execute();
	}

}
