package edu.uci.ics.textdb.exp.sampler;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.uci.ics.textdb.api.constants.TestConstants;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.api.utils.TestUtils;
import edu.uci.ics.textdb.exp.sampler.SamplerPredicate.SampleType;
import edu.uci.ics.textdb.exp.source.scan.ScanBasedSourceOperator;
import edu.uci.ics.textdb.exp.source.scan.ScanSourcePredicate;
import edu.uci.ics.textdb.storage.DataWriter;
import edu.uci.ics.textdb.storage.RelationManager;
import edu.uci.ics.textdb.storage.constants.LuceneAnalyzerConstants;

/**
 * @author Qinhua Huang
 */

public class SamplerTest {
    
    public static final String SAMPLER_TABLE = "sampler_test";
    private static int indexSize;
    @BeforeClass
    public static void setUp() throws TextDBException {
        RelationManager relationManager = RelationManager.getRelationManager();
        // Create the people table and write tuples
        
        RelationManager.getRelationManager().deleteTable(SAMPLER_TABLE);
        relationManager.createTable(SAMPLER_TABLE, "../index/test_tables/" + SAMPLER_TABLE, 
                TestConstants.SCHEMA_PEOPLE, LuceneAnalyzerConstants.standardAnalyzerString());
        DataWriter dataWriter = relationManager.getTableDataWriter(SAMPLER_TABLE);
        dataWriter.open();
        indexSize = 0;
        for (Tuple tuple : TestConstants.getSamplePeopleTuples()) {
            dataWriter.insertTuple(tuple);
            indexSize ++;
        }
        dataWriter.close();
    }
    
    @AfterClass
    public static void cleanUp() throws TextDBException {
        RelationManager.getRelationManager().deleteTable(SAMPLER_TABLE);
    }
    
    public static List<Tuple> computeSampleResults(String tableName, int k,
            SampleType sampleType) throws TextDBException {
        
        ScanBasedSourceOperator scanSource = new ScanBasedSourceOperator(new ScanSourcePredicate(tableName));
        Sampler tupleSampler = new Sampler(new SamplerPredicate(k, sampleType));
        tupleSampler.setInputOperator(scanSource);
        
        List<Tuple> results = new ArrayList<>();
        Tuple tuple;
        
        tupleSampler.open();
        while((tuple = tupleSampler.getNextTuple()) != null) {
            results.add(tuple);
        }
        tupleSampler.close();
        return results;
    }
    
    /*
     * To test if all the sampled tuples are in the sampler table.
     */
    public static boolean containedInSamplerTable(List<Tuple> sampleList) throws TextDBException {
        ScanBasedSourceOperator scanSource = 
                new ScanBasedSourceOperator(new ScanSourcePredicate(SAMPLER_TABLE));
        
        scanSource.open();
        Tuple nextTuple = null;
        List<Tuple> returnedTuples = new ArrayList<Tuple>();
        while ((nextTuple = scanSource.getNextTuple()) != null) {
            returnedTuples.add(nextTuple);
        }
        scanSource.close();
        boolean contains = TestUtils.containsAll(returnedTuples, sampleList);
        return contains;
    }
    
    /* 
     * To test if the sampled tuples are equal to the first K tuples of the sampler table 
     * in both the order and content.
     */
    public static boolean matchSamplerTable(List<Tuple> sampleList) throws TextDBException {
        ScanBasedSourceOperator scanSource = 
                new ScanBasedSourceOperator(new ScanSourcePredicate(SAMPLER_TABLE));
        
        scanSource.open();
        ListIterator<Tuple> iter = null;
        iter = sampleList.listIterator();
        while (iter.hasNext()) {
            Tuple nextTableTuple = scanSource.getNextTuple();
            Tuple nextSampledTuple = iter.next();
            
            if (!nextSampledTuple.equals(nextTableTuple)) {
                scanSource.close();
                return false;
            }
        }
        scanSource.close();
        return true;
    }
    
    /*
     * Sample 0 tuple in FIRST_K_ARRIVAL mode
     */
    @Test(expected = RuntimeException.class)
    public void test1() throws TextDBException {
        computeSampleResults(SAMPLER_TABLE,0, SampleType.FIRST_K_ARRIVAL);
    }
    
    /*
     * FIRST_K_ARRIVAL mode: sample the first tuple.
     */
    @Test
    public void test2() throws TextDBException {
        List<Tuple> results = computeSampleResults(SAMPLER_TABLE,1, SampleType.FIRST_K_ARRIVAL);
        Assert.assertEquals(results.size(), 1);
        Assert.assertTrue(matchSamplerTable(results));
    }
    
    /*
     * FIRST_K_ARRIVAL mode: Sample all the tuples.
     */
    @Test
    public void test3() throws TextDBException {
        List<Tuple> results = computeSampleResults(SAMPLER_TABLE,indexSize, SampleType.FIRST_K_ARRIVAL);
        Assert.assertEquals(results.size(), indexSize);
        Assert.assertTrue(matchSamplerTable(results));
    }
    
    /*
     * FIRST_K_ARRIVAL mode: the number of sampled records is 1 more than the table size.
     * It should return all the tuples from the table.
     */
    @Test
    public void test4() throws TextDBException {
        List<Tuple> results = computeSampleResults(SAMPLER_TABLE,indexSize+1, SampleType.FIRST_K_ARRIVAL);
        Assert.assertEquals(results.size(), indexSize);
        Assert.assertTrue(matchSamplerTable(results));
    }
    
    /*
     * RANDOM_SAMPLE mode: # of sampled tuples is in [0, tableSize].
     */
    @Test
    public void test5() throws TextDBException {
        List<Tuple> results = computeSampleResults(SAMPLER_TABLE,2, SampleType.RANDOM_SAMPLE);
        Assert.assertEquals(results.size(), 2);
        Assert.assertTrue(containedInSamplerTable(results));
    }
    
    /*
     * RANDOM_SAMPLE mode: sample zero tuple.
     */
    @Test(expected = RuntimeException.class)
    public void test6() throws TextDBException {
        computeSampleResults(SAMPLER_TABLE,0, SampleType.RANDOM_SAMPLE);
    }
    
    /*
     * RANDOM_SAMPLE mode: sample all tuples.
     * It should output all the tuples get from the table.
     */
    @Test
    public void test7() throws TextDBException {
        List<Tuple> results = computeSampleResults(SAMPLER_TABLE,indexSize, SampleType.RANDOM_SAMPLE);
        Assert.assertEquals(results.size(), indexSize);
        Assert.assertTrue(containedInSamplerTable(results));
    }
    
    /*
     * RANDOM_SAMPLE: # of sampled records is 1 + tableSize.
     * It should return all the tuples from the table.
     */
    @Test
    public void test8() throws TextDBException {
        List<Tuple> results = computeSampleResults(SAMPLER_TABLE,indexSize+1, SampleType.RANDOM_SAMPLE);
        Assert.assertEquals(results.size(), indexSize);
        Assert.assertTrue(containedInSamplerTable(results));
    }
}
