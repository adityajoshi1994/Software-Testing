package SoftwareTesting;

import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import edu.uci.ics.textdb.api.field.TextField;
import edu.uci.ics.textdb.api.schema.Attribute;
import edu.uci.ics.textdb.api.schema.AttributeType;
import edu.uci.ics.textdb.api.schema.Schema;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.exp.nlp.sentiment.NlpSentimentOperator;
import edu.uci.ics.textdb.exp.nlp.sentiment.NlpSentimentPredicate;
import edu.uci.ics.textdb.exp.nlp.sentiment.NlpSentimentTestConstants;
import edu.uci.ics.textdb.exp.sink.tuple.TupleSink;
import edu.uci.ics.textdb.exp.source.tuple.TupleSourceOperator;

public class BDD {
	
	private TupleSink tupleSink;
	
	public static String TEXT = "text";
    
    public static Attribute SentenceAttribute = new Attribute("text", AttributeType.TEXT);
    
    public static Schema SentenceSchema = new Schema(SentenceAttribute);
    
    public static Tuple positiveSentence = new Tuple(SentenceSchema, 
            new TextField("UCI is the best University"));
    
    public static Tuple neutralSentence = new Tuple(SentenceSchema,
            new TextField("I study in UCI"));
    
    public static Tuple negativeSentence = new Tuple(SentenceSchema,
            new TextField("The curricullum at UCI computer science is very tough."));
	
    public static Tuple veryPositiveSentence = new Tuple(SentenceSchema,
            new TextField("I am extremely delighted to study at UCI."));
    
	
	@Before
	public void setUp(){
		tupleSink = new TupleSink();		
	}
	
	@After
	public void tearDown(){
		tupleSink.close();
	}
	
	/**
	 * Given positive sentence
	 * applying sentiment analysis
	 * score should be 3.
	 */
	@Test
	public void positiveSentimentTest(){
		
		TupleSourceOperator tupleSource = new TupleSourceOperator(
                Arrays.asList(positiveSentence), SentenceSchema);
        NlpSentimentOperator sentiment = new NlpSentimentOperator(
                new NlpSentimentPredicate(TEXT, "sentiment"));
        
        
        sentiment.setInputOperator(tupleSource);
        tupleSink.setInputOperator(sentiment);
        tupleSink.open();
        
        List<Tuple> results = tupleSink.collectAllTuples();
        
        Tuple tuple = results.get(0);
        Assert.assertEquals(tuple.getField("sentiment").getValue(), 3);
        
	}
	
	
	/**
	 * Given neutral sentence
	 * applying sentiment analysis
	 * score should be 2.
	 */
	
	@Test
	public void neutralSentimentTest(){
		TupleSourceOperator tupleSource = new TupleSourceOperator(
                Arrays.asList(neutralSentence), SentenceSchema);
        NlpSentimentOperator sentiment = new NlpSentimentOperator(
                new NlpSentimentPredicate(TEXT, "sentiment"));
    
        
        sentiment.setInputOperator(tupleSource);
        tupleSink.setInputOperator(sentiment);
        
        tupleSink.open();
        List<Tuple> results = tupleSink.collectAllTuples();
        
        Tuple tuple = results.get(0);
        Assert.assertEquals(tuple.getField("sentiment").getValue(), 2); 
	}
	
	/**
	 * Given negative sentence
	 * applying sentiment analysis
	 * score should be 1.
	 */
	
	@Test
	public void negativeSentimentTest(){
		TupleSourceOperator tupleSource = new TupleSourceOperator(
                Arrays.asList(negativeSentence), SentenceSchema);
        NlpSentimentOperator sentiment = new NlpSentimentOperator(
                new NlpSentimentPredicate(TEXT, "sentiment"));
    
        
        sentiment.setInputOperator(tupleSource);
        tupleSink.setInputOperator(sentiment);
        
        tupleSink.open();
        List<Tuple> results = tupleSink.collectAllTuples();
        
        Tuple tuple = results.get(0);
        Assert.assertEquals(tuple.getField("sentiment").getValue(), 1); 
	}
	
	/**
	 * Given very positive sentence
	 * applying sentiment analysis
	 * score should be 4.
	 */
	
	@Test
	public void veryPositiveSentimentTest(){
		TupleSourceOperator tupleSource = new TupleSourceOperator(
                Arrays.asList(veryPositiveSentence), SentenceSchema);
        NlpSentimentOperator sentiment = new NlpSentimentOperator(
                new NlpSentimentPredicate(TEXT, "sentiment"));
    
        
        sentiment.setInputOperator(tupleSource);
        tupleSink.setInputOperator(sentiment);
        
        tupleSink.open();
        List<Tuple> results = tupleSink.collectAllTuples();
        
        Tuple tuple = results.get(0);
        Assert.assertEquals(tuple.getField("sentiment").getValue(), 4); 
	}
	
}
