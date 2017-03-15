package notaql.extensions.advisor.statistics;

public class RuntimePredictionResult {
	public final long runtimeMillis;
	public final double similarity;
	
	
	public RuntimePredictionResult(long runtimeMillis, double similarity) {
		this.runtimeMillis = runtimeMillis;
		this.similarity = similarity;
	}
}
