package flink.examples.utils;

import static java.util.Objects.requireNonNull;

import java.io.Serializable;
import java.util.Iterator;

/**
 * A variant of the collection source (emits a sequence of elements as a stream)
 * that supports throttling the emission rate.
 * @param <T>
 */
public class ThrottledIterator<T> implements Iterator<T>, Serializable {
    
    private static final long serialVersionUID = 1L;

    private final Iterator<T> source;

    private final long sleepBatchSize;
	private final long sleepBatchTime;

	private long lastBatchCheckTime;
	private long num;

    public ThrottledIterator(Iterator<T> source, long elementsPerSecond) {
        this.source = requireNonNull(source);

        if (!(source instanceof Serializable)) {
            throw new IllegalArgumentException("source must be java.io.Serializable");
        }

        if (elementsPerSecond >= 100) {
            this.sleepBatchSize = elementsPerSecond / 20;
            this.sleepBatchTime = 50;
        } else if (elementsPerSecond >= 1) {
            this.sleepBatchSize = 1;
            this.sleepBatchTime = 1000 / elementsPerSecond;
        } else {
            throw new IllegalArgumentException("'elements per second' must be positive and not zero");
        }
    }

    @Override
    public boolean hasNext() {
        return source.hasNext();
    }

    @Override
    public T next() {
        if (lastBatchCheckTime > 0) {
            if (++num >= sleepBatchSize) {
                num = 0;

                final long now = System.currentTimeMillis();
                final long elapsed = now - lastBatchCheckTime;
                if (elapsed < sleepBatchTime) {
                    try {
                        Thread.sleep(sleepBatchTime - elapsed);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                lastBatchCheckTime = now;
            }
        } else {
            lastBatchCheckTime = System.currentTimeMillis();
        }

        return source.next();
    }

    @Override
	public void remove() {
		throw new UnsupportedOperationException();
	}
}
