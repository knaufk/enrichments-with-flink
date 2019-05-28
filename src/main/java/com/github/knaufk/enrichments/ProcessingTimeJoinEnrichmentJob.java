package com.github.knaufk.enrichments;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.github.knaufk.enrichments.entities.EnrichedMeasurements;
import com.github.knaufk.enrichments.entities.SensorMeasurement;
import com.github.knaufk.enrichments.entities.SensorReferenceData;
import com.github.knaufk.enrichments.source.SensorMeasurementSource;
import com.github.knaufk.enrichments.source.SensorReferenceDataSource;
import com.github.knaufk.enrichments.streams.ProcessingTimeJoin;

public class ProcessingTimeJoinEnrichmentJob {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.enableCheckpointing(5000);

		DataStream<SensorMeasurement> measurements = env.addSource(new SensorMeasurementSource(
				100_000));

		DataStream<SensorReferenceData> referenceData = env.addSource(new SensorReferenceDataSource(
				10_000));

		DataStream<EnrichedMeasurements> enrichedMeasurements = measurements.keyBy(m -> m.getSensorId())
																			.connect(referenceData.keyBy(r -> r.getSensorId()))
																			.process(new ProcessingTimeJoin());

		enrichedMeasurements.print();

		env.execute();
	}

	private static class SensorIdPartitioner implements Partitioner<Long> {
		@Override
		public int partition(final Long sensorMeasurement, final int numPartitions) {
			return Math.toIntExact(sensorMeasurement % numPartitions);
		}
	}
}
