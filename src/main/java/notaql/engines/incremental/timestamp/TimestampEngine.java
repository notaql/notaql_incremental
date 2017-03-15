package notaql.engines.incremental.timestamp;

import notaql.engines.incremental.IncrementalEngine;

public interface TimestampEngine extends IncrementalEngine {
	public String getTimestampParameterName();
}
