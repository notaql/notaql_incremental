package notaql.engines.incremental.snapshot;

import notaql.datamodel.AtomValue;
import notaql.engines.DatabaseReference;
import notaql.engines.incremental.IncrementalEngine;

public interface SnapshotEngine extends IncrementalEngine {
	public String getSnapshotParameterName();
	
	
	/**
	 * @return the value to be used for the snapshot parameter
	 */
	public abstract AtomValue<?> getSnapshotValue(DatabaseReference databaseReference);
}
