package notaql.engines.hbase.exceptions;

import notaql.model.EvaluationException;

/**
 * Thrown when the user starts a timestamp-based getDataFiltered() but the target-table has no
 * versions for the objects.
 */
public class TableHasNoVersionsException extends EvaluationException {
	private static final long serialVersionUID = -6722181320227798975L;

	public TableHasNoVersionsException() {
    }

    public TableHasNoVersionsException(String message) {
        super(message);
    }

    public TableHasNoVersionsException(String message, Throwable cause) {
        super(message, cause);
    }

    public TableHasNoVersionsException(Throwable cause) {
        super(cause);
    }
}
