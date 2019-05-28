package com.github.knaufk.enrichments.entities;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class SensorMeasurement {

	private long timestamp;
	private long sensorId;
	private double value;

}
