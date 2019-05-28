package com.github.knaufk.enrichments.source;

import com.github.knaufk.enrichments.entities.SensorReferenceData;
import com.github.knaufk.enrichments.io.SensorReferenceDataClient;

import java.util.SplittableRandom;

public class SensorReferenceDataSource extends BaseGenerator<SensorReferenceData> {

	public SensorReferenceDataSource(final int maxRecordsPerSecond) {
		super(maxRecordsPerSecond);
	}

	@Override
	protected SensorReferenceData randomEvent(final SplittableRandom rnd, final long id) {
		return SensorReferenceDataClient.getRandomReferenceDataFor(rnd.nextLong(SensorMeasurementSource.SENSOR_COUNT));
	}
}
