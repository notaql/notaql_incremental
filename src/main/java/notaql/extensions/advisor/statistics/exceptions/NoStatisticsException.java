package notaql.extensions.advisor.statistics.exceptions;

public class NoStatisticsException extends Exception {
	private static final long serialVersionUID = 5799273404157316919L;

	public NoStatisticsException() {
		super();
	}

	public NoStatisticsException(String message) {
		super(message);
	}

	public NoStatisticsException(String message, Throwable cause) {
		super(message, cause);
	}

	public NoStatisticsException(Throwable cause) {
		super(cause);
	}
}
