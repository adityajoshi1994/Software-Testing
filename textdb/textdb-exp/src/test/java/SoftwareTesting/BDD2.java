package SoftwareTesting;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.field.IField;
import edu.uci.ics.textdb.api.field.TextField;
import edu.uci.ics.textdb.api.schema.Attribute;
import edu.uci.ics.textdb.api.schema.AttributeType;
import edu.uci.ics.textdb.api.schema.Schema;
import edu.uci.ics.textdb.api.span.Span;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.api.utils.TestUtils;
import edu.uci.ics.textdb.api.utils.Utils;
import edu.uci.ics.textdb.exp.nlp.entity.NlpEntityOperator;
import edu.uci.ics.textdb.exp.nlp.entity.NlpEntityPredicate;
import edu.uci.ics.textdb.exp.nlp.entity.NlpEntityTestConstants;
import edu.uci.ics.textdb.exp.nlp.entity.NlpEntityType;
import edu.uci.ics.textdb.exp.source.scan.ScanBasedSourceOperator;
import edu.uci.ics.textdb.exp.source.scan.ScanSourcePredicate;
import edu.uci.ics.textdb.exp.utils.DataflowUtils;
import edu.uci.ics.textdb.storage.DataWriter;
import edu.uci.ics.textdb.storage.RelationManager;
import edu.uci.ics.textdb.storage.constants.LuceneAnalyzerConstants;

public class BDD2 {
	
	public static final String SENTENCE_ONE = "sentence_one";
    public static final String SENTENCE_TWO = "sentence_two";
    
    public static final String TWO_SENTENCE_TABLE = "nlp_test_one_sentence";

    public static final Attribute SENTENCE_ONE_ATTR = new Attribute(SENTENCE_ONE, AttributeType.TEXT);

    public static final Attribute SENTENCE_TWO_ATTR = new Attribute(SENTENCE_TWO, AttributeType.TEXT);

	
    public static final Schema SCHEMA_TWO_SENTENCE = new Schema(SENTENCE_ONE_ATTR, SENTENCE_TWO_ATTR);

    public static final String RESULTS = "nlp entity";
	
    
    @BeforeClass
    public static void setUp() throws TextDBException {
        RelationManager relationManager = RelationManager.getRelationManager();
        relationManager.createTable(TWO_SENTENCE_TABLE, "../index/test_tables/" + TWO_SENTENCE_TABLE, 
                NlpEntityTestConstants.SCHEMA_TWO_SENTENCE, LuceneAnalyzerConstants.standardAnalyzerString());
    }
    
    
    @AfterClass
    public static void cleanUp() throws Exception {
        RelationManager relationManager = RelationManager.getRelationManager();
        relationManager.deleteTable(TWO_SENTENCE_TABLE);
    }
    
    @After
    public void deleteData() throws TextDBException {
        RelationManager relationManager = RelationManager.getRelationManager();
        DataWriter twoSentenceDataWriter = relationManager.getTableDataWriter(TWO_SENTENCE_TABLE);
        twoSentenceDataWriter.open();
        twoSentenceDataWriter.clearData();
        twoSentenceDataWriter.close();
    }
    
	public static List<Tuple> Tuple() throws ParseException {

        IField[] fields1 = { new TextField("UCI, UCB are good universities."),
                new TextField("Irvine and Berkeley are cities.") };
        Tuple tuple1 = new Tuple(SCHEMA_TWO_SENTENCE, fields1);
        return Arrays.asList(tuple1);
        
    }
	
	public static List<Tuple> Tuple1() throws ParseException {

        IField[] fields1 = { new TextField("Bill is a good guy"),
                new TextField("Tim is also good.") };
        Tuple tuple1 = new Tuple(SCHEMA_TWO_SENTENCE, fields1);
        return Arrays.asList(tuple1);
        
    }
	
	public static List<Tuple> locationResult() {
        List<Tuple> resultList = new ArrayList<>();

        List<Span> spanList = new ArrayList<Span>();

        Span span1 = new Span("sentence_two", 0, 6, NlpEntityType.LOCATION.toString(), "Irvine");
        Span span2 = new Span("sentence_two", 11, 19, NlpEntityType.LOCATION.toString(), "Berkeley");
        

        spanList.add(span1);
        spanList.add(span2);
        
        IField[] fields1 = { new TextField("UCI, UCB are good universities."),
                new TextField("Irvine and Berkeley are cities.") };
        Tuple tuple1 = new Tuple(SCHEMA_TWO_SENTENCE, fields1);


        Schema returnSchema = Utils.addAttributeToSchema(tuple1.getSchema(), new Attribute(RESULTS, AttributeType.LIST));

        Tuple returnTuple = DataflowUtils.getSpanTuple(tuple1.getFields(), spanList, returnSchema);
        resultList.add(returnTuple);

        return resultList;
    }
	
	
	public static List<Tuple> personResult() {
        List<Tuple> resultList = new ArrayList<>();

        List<Span> spanList = new ArrayList<Span>();

        Span span1 = new Span("sentence_one", 0, 4, NlpEntityType.PERSON.toString(), "Bill");
        Span span2 = new Span("sentence_two", 0, 3, NlpEntityType.PERSON.toString(), "Tim");
        

        spanList.add(span1);
        spanList.add(span2);
        
        IField[] fields1 = { new TextField("Bill is a good guy"),
                new TextField("Tim is also good.") };
        Tuple tuple1 = new Tuple(SCHEMA_TWO_SENTENCE, fields1);


        Schema returnSchema = Utils.addAttributeToSchema(tuple1.getSchema(), new Attribute(RESULTS, AttributeType.LIST));

        Tuple returnTuple = DataflowUtils.getSpanTuple(tuple1.getFields(), spanList, returnSchema);
        resultList.add(returnTuple);

        return resultList;
    }

	
	public static List<Tuple> organisationResult() {
        List<Tuple> resultList = new ArrayList<>();

        List<Span> spanList = new ArrayList<Span>();

        Span span1 = new Span("sentence_one", 0, 3, NlpEntityType.ORGANIZATION.toString(), "UCI");
        Span span2 = new Span("sentence_one", 5, 8, NlpEntityType.ORGANIZATION.toString(), "UCB");
        

        spanList.add(span1);
        spanList.add(span2);
        
        IField[] fields1 = { new TextField("UCI, UCB are good universities."),
                new TextField("Irvine and Berkeley are cities.") };
        Tuple tuple1 = new Tuple(SCHEMA_TWO_SENTENCE, fields1);


        Schema returnSchema = Utils.addAttributeToSchema(tuple1.getSchema(), new Attribute(RESULTS, AttributeType.LIST));

        Tuple returnTuple = DataflowUtils.getSpanTuple(tuple1.getFields(), spanList, returnSchema);
        resultList.add(returnTuple);

        return resultList;
    }
	
	public static List<Tuple> adjectiveResult() {
        List<Tuple> resultList = new ArrayList<>();

        List<Span> spanList = new ArrayList<Span>();

        Span span1 = new Span("sentence_one", 13, 17, NlpEntityType.ADJECTIVE.toString(), "good");
        
        

        spanList.add(span1);
        
        
        IField[] fields1 = { new TextField("UCI, UCB are good universities."),
                new TextField("Irvine and Berkeley are cities.") };
        Tuple tuple1 = new Tuple(SCHEMA_TWO_SENTENCE, fields1);


        Schema returnSchema = Utils.addAttributeToSchema(tuple1.getSchema(), new Attribute(RESULTS, AttributeType.LIST));

        Tuple returnTuple = DataflowUtils.getSpanTuple(tuple1.getFields(), spanList, returnSchema);
        resultList.add(returnTuple);

        return resultList;
    }
	
	
	
	public List<Tuple> getQueryResults(String tableName, List<String> attributeNames,
            NlpEntityType nlpEntityType, int limit, int offset) throws Exception {
        
        ScanBasedSourceOperator scanSource = new ScanBasedSourceOperator(new ScanSourcePredicate(tableName));

        NlpEntityPredicate nlpEntityPredicate = new NlpEntityPredicate(nlpEntityType, attributeNames, RESULTS);
        NlpEntityOperator nlpEntityOperator = new NlpEntityOperator(nlpEntityPredicate);
        nlpEntityOperator.setInputOperator(scanSource);

        nlpEntityOperator.setLimit(limit);
        nlpEntityOperator.setOffset(offset);
        
        Tuple nextTuple = null;
        List<Tuple> results = new ArrayList<Tuple>();
        
        nlpEntityOperator.open();
        while ((nextTuple = nlpEntityOperator.getNextTuple()) != null) {
            results.add(nextTuple);
        }
        nlpEntityOperator.close();
        
        return results;
    }
	
	/**
     * Given the following sentence 
     * UCI, UCB are good universities.
     * Irvine and Berkley are cities.
     * Applying POS tagger
     * should return the tags for UCI and UCB as organisations. 
     */
    @Test
    public void organizationSearch() throws Exception {
        List<Tuple> data = Tuple();

        DataWriter twoSentenceDataWriter = RelationManager.getRelationManager().getTableDataWriter(TWO_SENTENCE_TABLE);
        twoSentenceDataWriter.open();
        for (Tuple tuple : data) {
            twoSentenceDataWriter.insertTuple(tuple);
        }
        twoSentenceDataWriter.close();

        String attribute1 = SENTENCE_ONE;
        String attribute2 = SENTENCE_TWO;

        List<String> attributeNames = new ArrayList<>();
        attributeNames.add(attribute1);
        attributeNames.add(attribute2);

        List<Tuple> returnedResults = getQueryResults(TWO_SENTENCE_TABLE, attributeNames,
                NlpEntityType.ORGANIZATION,Integer.MAX_VALUE, 0);

        List<Tuple> expectedResults = organisationResult();

        boolean contains = TestUtils.equals(expectedResults, returnedResults);
        
        Assert.assertTrue(contains);
    }
    
    /**
    * Given the following sentence 
    * UCI, UCB are good universities.
    * Irvine and Berkley are cities.
    * Applying POS tagger
    * should return the tags for good as adjective. 
    */
    @Test
    public void adjectiveSearch() throws Exception {
        List<Tuple> data = Tuple();

        DataWriter twoSentenceDataWriter = RelationManager.getRelationManager().getTableDataWriter(TWO_SENTENCE_TABLE);
        twoSentenceDataWriter.open();
        for (Tuple tuple : data) {
            twoSentenceDataWriter.insertTuple(tuple);
        }
        twoSentenceDataWriter.close();

        String attribute1 = SENTENCE_ONE;
        String attribute2 = SENTENCE_TWO;

        List<String> attributeNames = new ArrayList<>();
        attributeNames.add(attribute1);
        attributeNames.add(attribute2);

        List<Tuple> returnedResults = getQueryResults(TWO_SENTENCE_TABLE, attributeNames,
                NlpEntityType.ADJECTIVE,Integer.MAX_VALUE, 0);

        List<Tuple> expectedResults = adjectiveResult();

        boolean contains = TestUtils.equals(expectedResults, returnedResults);
        
        Assert.assertTrue(contains);
    }
    
    
    /**
     * Given the following sentence 
     * UCI, UCB are good universities.
     * Irvine and Berkley are cities.
     * Applying POS tagger
     * should return the tags for Irvine and Berkeley as location. 
     */
     @Test
     public void locationSearch() throws Exception {
         List<Tuple> data = Tuple();

         DataWriter twoSentenceDataWriter = RelationManager.getRelationManager().getTableDataWriter(TWO_SENTENCE_TABLE);
         twoSentenceDataWriter.open();
         for (Tuple tuple : data) {
             twoSentenceDataWriter.insertTuple(tuple);
         }
         twoSentenceDataWriter.close();

         String attribute1 = SENTENCE_ONE;
         String attribute2 = SENTENCE_TWO;

         List<String> attributeNames = new ArrayList<>();
         attributeNames.add(attribute1);
         attributeNames.add(attribute2);

         List<Tuple> returnedResults = getQueryResults(TWO_SENTENCE_TABLE, attributeNames,
                 NlpEntityType.LOCATION,Integer.MAX_VALUE, 0);

         List<Tuple> expectedResults = locationResult();

         boolean contains = TestUtils.equals(expectedResults, returnedResults);
         
         Assert.assertTrue(contains);
     }

     /**
      * Given the following sentence 
      * UCI, UCB are good universities.
      * Irvine and Berkley are cities.
      * Applying POS tagger
      * should return the tags for Irvine and Berkeley as location. 
      */
      @Test
      public void personSearch() throws Exception {
          List<Tuple> data = Tuple1();

          DataWriter twoSentenceDataWriter = RelationManager.getRelationManager().getTableDataWriter(TWO_SENTENCE_TABLE);
          twoSentenceDataWriter.open();
          for (Tuple tuple : data) {
              twoSentenceDataWriter.insertTuple(tuple);
          }
          twoSentenceDataWriter.close();

          String attribute1 = SENTENCE_ONE;
          String attribute2 = SENTENCE_TWO;

          List<String> attributeNames = new ArrayList<>();
          attributeNames.add(attribute1);
          attributeNames.add(attribute2);

          List<Tuple> returnedResults = getQueryResults(TWO_SENTENCE_TABLE, attributeNames,
                  NlpEntityType.PERSON,Integer.MAX_VALUE, 0);

          List<Tuple> expectedResults = personResult();

          boolean contains = TestUtils.equals(expectedResults, returnedResults);
          
          Assert.assertTrue(contains);
      }
     
     
     
}
