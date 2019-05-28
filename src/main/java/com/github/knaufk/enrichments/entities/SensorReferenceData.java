package com.github.knaufk.enrichments.entities;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class SensorReferenceData {

	private long sensorId;
	private double lon;
	private double lat;
	private String importantMetadata;
	private String moreImportantMetadata;

}
