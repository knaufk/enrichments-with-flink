package com.github.knaufk.enrichments.streams;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import com.github.knaufk.enrichments.entities.EnrichedMeasurements;
import com.github.knaufk.enrichments.entities.SensorMeasurement;
import com.github.knaufk.enrichments.entities.SensorReferenceData;

public class ProcessingTimeJoin extends CoProcessFunction<SensorMeasurement, SensorReferenceData, EnrichedMeasurements> {

	// Store latest reference data
	private ValueState<SensorReferenceData> referenceDataState = null;

	@Override
	public void open(Configuration config) {
		ValueStateDescriptor<SensorReferenceData> cDescriptor = new ValueStateDescriptor<>(
				"referenceData",
				TypeInformation.of(SensorReferenceData.class)
		);
		referenceDataState = getRuntimeContext().getState(cDescriptor);
	}

	@Override
	public void processElement1(SensorMeasurement measurement,
	                            Context context,
	                            Collector<EnrichedMeasurements> out) throws Exception {
		out.collect(new EnrichedMeasurements(measurement, referenceDataState.value()));
	}

	@Override
	public void processElement2(
			SensorReferenceData referenceData,
			Context context,
			Collector<EnrichedMeasurements> collector) throws Exception {
		referenceDataState.update(referenceData);
	}
}
