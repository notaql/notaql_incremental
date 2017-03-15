package notaql.engines.mongodb.exceptions;

import notaql.model.EvaluationException;

/**
 * Thrown when the user starts a timestamp-based getDataFiltered() but the database contains documents with
 * a user-defined object id. A timestamp-based query would just ignore these documents which could lead to
 * unexpected errors.  
 */
public class UserDefinedOIDException extends EvaluationException {
	private static final long serialVersionUID = -1795746577931054662L;

    public UserDefinedOIDException() {
    }

    public UserDefinedOIDException(String message) {
        super(message);
    }

    public UserDefinedOIDException(String message, Throwable cause) {
        super(message, cause);
    }

    public UserDefinedOIDException(Throwable cause) {
        super(cause);
    }
}
