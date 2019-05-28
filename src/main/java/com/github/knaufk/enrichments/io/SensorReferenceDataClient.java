package com.github.knaufk.enrichments.io;

import com.github.knaufk.enrichments.entities.SensorReferenceData;
import org.apache.commons.lang3.RandomStringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.github.knaufk.enrichments.source.SensorMeasurementSource.SENSOR_COUNT;

public class SensorReferenceDataClient {

    private static final Random RAND = new Random(42);

    private static ExecutorService pool = Executors.newFixedThreadPool(30,
            new ThreadFactory() {
                private final ThreadFactory threadFactory = Executors.defaultThreadFactory();

                @Override
                public Thread newThread(Runnable r) {
                    Thread thread = threadFactory.newThread(r);
                    thread.setName("temp-client-" + thread.getName());
                    return thread;
                }
            });


    public SensorReferenceData getSensorReferenceDataFor(long sensorId) throws Exception {
        return new SensorReferenceDataSupplier(sensorId).get();
    }

    public void asyncGetSensorReferenceDataFor(long sensorId, Consumer<SensorReferenceData> callback) {

        CompletableFuture.supplyAsync(new SensorReferenceDataSupplier(sensorId), pool)
                .thenAcceptAsync(callback,com.google.common.util.concurrent.MoreExecutors.directExecutor());
    }

	public Map<Long, SensorReferenceData> getSensorReferenceData() {
		final Map<Long, SensorReferenceData> output = new HashMap<>();

		for (long i = 0; i < SENSOR_COUNT; i++) {
			output.put(i, getRandomReferenceDataFor(i));
		}

		return output;
	}

	public Map<Long, SensorReferenceData> getSensorReferenceDataForPartition(
			final int partition,
			final int numPartitions) {
		final Map<Long, SensorReferenceData> output = new HashMap<>();

		for (long i = 0; i < SENSOR_COUNT; i++) {
			if (i % numPartitions == partition) {
				output.put(i, getRandomReferenceDataFor(i));
			}
		}

		return output;
	}

	private static class SensorReferenceDataSupplier implements Supplier<SensorReferenceData> {

	    private long sensorId;

	    public SensorReferenceDataSupplier(final long sensorId) {
		    this.sensorId = sensorId;
	    }

	    @Override
        public SensorReferenceData get() {
            try {
                Thread.sleep(RAND.nextInt(5) + 5);
            } catch (InterruptedException e) {
                //Swallowing Interruption here
            }
            return getRandomReferenceDataFor(sensorId);
        }


	}

	public static SensorReferenceData getRandomReferenceDataFor(long sensorId) {
		return SensorReferenceData.builder().sensorId(sensorId)
		                          .lat(RAND.nextDouble() * 180 - 90)
		                          .lon(RAND.nextDouble() * 180)
		                          .importantMetadata(RandomStringUtils.randomAlphanumeric(20).toUpperCase())
		                          .moreImportantMetadata(RandomStringUtils.randomAlphanumeric(20).toUpperCase())
		                          .build();
	}
}
