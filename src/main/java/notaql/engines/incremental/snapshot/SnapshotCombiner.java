package notaql.engines.incremental.snapshot;

import java.util.List;

import com.google.common.base.Optional;

import notaql.datamodel.ObjectValue;
import scala.Tuple2;

public interface SnapshotCombiner {
	/**
	 * Combines two elements.
	 * 
	 * @param tuple the two ObjectValues
	 * @return the combination of both elements
	 */
	public List<ObjectValue> combine(Tuple2<Optional<ObjectValue>, Optional<ObjectValue>> tuple);
}
