package notaql.engines.incremental.util;

import notaql.model.AttributeSpecification;
import notaql.model.vdata.ArithmeticVData;
import notaql.model.vdata.ObjectVData;
import notaql.model.vdata.VData;
import notaql.model.vdata.aggregation.AggregatingVData;
import notaql.model.vdata.aggregation.CountVData;
import notaql.model.vdata.aggregation.ListVData;
import notaql.model.vdata.aggregation.SimpleAggregatingVData;
import notaql.model.vdata.aggregation.SumVData;

public class Util {
	/**
	 * Checks if the query contains only functions which support incremental computations (SUM() / COUNT() / OBJECT()).
	 * 
	 * @param queryExpression
	 * @return
	 */
	public static boolean isQuerySupportingIncrementalComputation(VData queryExpression) {
		return isQuerySupportingIncrementalComputation(queryExpression, false);
	}
	
	
	/**
	 * Checks if the query contains only functions which support incremental computations (SUM() / COUNT() / OBJECT()).
	 * 
	 * @param queryExpression
	 * @param isNested if the recursion is at a point where every queryExpression would be nested and by this invalid if it is a function. Needed because NotaQL always wraps everything in an aggregating function.
	 * @return
	 */
	private static boolean isQuerySupportingIncrementalComputation(VData queryExpression, boolean isNested) {
		if (queryExpression instanceof ObjectVData) {
			for (AttributeSpecification specification : ((ObjectVData) queryExpression).getSpecifications()) {
				VData vdata = specification.getVData();
				if (!isQuerySupportingIncrementalComputation(vdata, isNested))
					return false;
			}
			return true;
		}
		
		else if (queryExpression instanceof SimpleAggregatingVData) {
			if (queryExpression instanceof SumVData || queryExpression instanceof CountVData) {
				if (isNested)
					return false;
				else {
					VData vdata = ((SimpleAggregatingVData<?>) queryExpression).getExpression();
					return isQuerySupportingIncrementalComputation(vdata, true);
				}
			}
			else
				return false;
		}
		
		else if (queryExpression instanceof ListVData)
			return false;
		
		else if (queryExpression instanceof ArithmeticVData) {
			// OUT.x <- SUM()+1 would break everything when the algorithm tries to combine the old and the new result => this is a nested expression
			return isQuerySupportingIncrementalComputation(((ArithmeticVData) queryExpression).getLeft(), true)
					&& isQuerySupportingIncrementalComputation(((ArithmeticVData) queryExpression).getRight(), true); 
		}
			
		else
			return true;
	}
	
	
	/**
	 * Checks if the query contains aggregating functions
	 * 
	 * @param queryExpression
	 * @return
	 */
	public static boolean isQueryAggregating(VData queryExpression) {
		if (queryExpression instanceof ObjectVData) {
			for (AttributeSpecification specification : ((ObjectVData) queryExpression).getSpecifications()) {
				VData vdata = specification.getVData();
				if (isQueryAggregating(vdata))
					return true;
			}
			return false;
		}
		
		else if (queryExpression instanceof SimpleAggregatingVData || queryExpression instanceof AggregatingVData)
			return true;
		
		else if (queryExpression instanceof ArithmeticVData)
			return isQueryAggregating(((ArithmeticVData) queryExpression).getLeft())
						|| isQueryAggregating(((ArithmeticVData) queryExpression).getRight());
			
		else
			return false;
	}
}
