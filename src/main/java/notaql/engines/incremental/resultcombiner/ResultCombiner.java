package notaql.engines.incremental.resultcombiner;

import com.google.common.base.Optional;

import notaql.datamodel.ObjectValue;
import scala.Tuple2;

public interface ResultCombiner {
	/**
	 * Combines two elements.
	 * 
	 * @param elements key is the row-id, value contains two ObjectValues
	 * @return the combination of both elements
	 */
	public ObjectValue combine(Tuple2<Optional<ObjectValue>, Optional<ObjectValue>> tuple);
}
