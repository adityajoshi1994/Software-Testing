package edu.uci.ics.textdb.exp.join;

import edu.uci.ics.textdb.api.dataflow.IPredicate;
import edu.uci.ics.textdb.api.exception.DataFlowException;
import edu.uci.ics.textdb.api.schema.Schema;
import edu.uci.ics.textdb.api.tuple.Tuple;

/**
 * IJoinOperator is the interface for the classes implementing predicates for 
 * the Join Operator.
 * 
 * @author sripadks
 */
public interface IJoinPredicate extends IPredicate {

	Tuple joinTuples(Tuple innerTuple, Tuple outerTuple, Schema outputSchema)
			throws Exception;
	
	Schema generateOutputSchema(Schema innerOperatorSchema, Schema outerOperatorSchema) throws DataFlowException;
	
	String getInnerAttributeName();
	
	String getOuterAttributeName();
}