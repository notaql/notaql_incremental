package notaql.extensions;

public class Util {
	public static long normalize(long value, long min, long max) {
		return (value-min)/(max-min);
	}
	
	
	public static double normalize(double value, double min, double max) {
		return (value-min)/(max-min);
	}
}
