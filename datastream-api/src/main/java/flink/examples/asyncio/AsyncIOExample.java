package flink.examples.asyncio;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncIOExample {
    private static final Logger LOG = LoggerFactory.getLogger(AsyncIOExample.class);

	private static final String EXACTLY_ONCE_MODE = "exactly_once";
	private static final String EVENT_TIME = "EventTime";
	private static final String INGESTION_TIME = "IngestionTime";
	private static final String ORDERED = "ordered";
}
