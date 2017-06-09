package SoftwareTesting;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import edu.uci.ics.textdb.api.constants.SchemaConstants;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.api.schema.Attribute;
import edu.uci.ics.textdb.api.schema.AttributeType;
import edu.uci.ics.textdb.api.schema.Schema;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.exp.sink.tuple.TupleSink;
import edu.uci.ics.textdb.exp.sink.tuple.TupleSinkPredicate;
import junit.framework.Assert;

public class Mockito2 {
	
	private IOperator IPOperator;
    private Schema IPSchema = new Schema(
            SchemaConstants._ID_ATTRIBUTE, new Attribute("content", AttributeType.STRING), SchemaConstants.PAYLOAD_ATTRIBUTE);

	@Before
	public void setUp(){
		IPOperator = Mockito.mock(IOperator.class);
        Mockito.when(IPOperator.getOutputSchema()).thenReturn(IPSchema);
	}
	
	/**
	 * Test open and close of tupleSink operator
	 * @throws Exception
	 */
	@Test
    public void OpenCloseTest() throws Exception {
        TupleSink tupleSink = new TupleSink();
        tupleSink.setInputOperator(IPOperator);
        
        tupleSink.open();
        
        Mockito.verify(IPOperator).open();
        Assert.assertEquals(
                new Schema(SchemaConstants._ID_ATTRIBUTE, new Attribute("content", AttributeType.STRING)), 
                tupleSink.getOutputSchema());
        
        tupleSink.close();
        Mockito.verify(IPOperator).close();
    }
	
	/**
	 * Test get next tuple of tupleSink operator on STRING attribute
	 * @throws Exception
	 */
	 @Test
	    public void getNextTupleTest() throws Exception {
	        TupleSink tupleSink = new TupleSink();
	        tupleSink.setInputOperator(IPOperator);
	        
	        Tuple sampleTuple = Mockito.mock(Tuple.class);
	        Mockito.when(sampleTuple.toString()).thenReturn("Sample Tuple");
	        Mockito.when(sampleTuple.getSchema()).thenReturn(IPSchema);
	        
	        Mockito.when(IPOperator.getNextTuple()).thenReturn(sampleTuple).thenReturn(null);
	        tupleSink.open();
	        tupleSink.getNextTuple();
	        Mockito.verify(IPOperator, Mockito.times(1)).getNextTuple();

	        tupleSink.close();
	        
	    }
	 
	 
	 	/**
	 	 * Limit test of Tuple Sink on STRING attribute
	 	 * @throws Exception
	 	 */
	    @Test
	    public void testLimitOffset() throws Exception {
	        TupleSink tupleSink = new TupleSink(new TupleSinkPredicate(1, 1));
	        tupleSink.setInputOperator(IPOperator);
	        
	        Tuple sampleTuple1 = Mockito.mock(Tuple.class);
	        Mockito.when(sampleTuple1.getSchema()).thenReturn(IPSchema);      
	        Tuple sampleTuple2 = Mockito.mock(Tuple.class);
	        Mockito.when(sampleTuple2.getSchema()).thenReturn(IPSchema);
	        Tuple sampleTuple3 = Mockito.mock(Tuple.class);
	        Mockito.when(sampleTuple3.getSchema()).thenReturn(IPSchema);
	        Mockito.when(IPOperator.getNextTuple()).thenReturn(sampleTuple1)
	            .thenReturn(sampleTuple2).thenReturn(sampleTuple3).thenReturn(null);
	        
	        tupleSink.open();
	        Tuple resultTuple1 = tupleSink.getNextTuple();
	        Tuple resultTuple2 = tupleSink.getNextTuple();
	        tupleSink.close();
	        Mockito.verify(IPOperator,Mockito.times(2)).getNextTuple();
	        Assert.assertTrue(resultTuple1 != null);
	        Assert.assertTrue(resultTuple2 == null);
	        
	    }
	
	
}
