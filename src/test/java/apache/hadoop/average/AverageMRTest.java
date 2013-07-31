package apache.hadoop.average;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class AverageMRTest {

    /*
     * Declare harnesses that let you test a mapper, a reducer, and
     * a mapper and a reducer working together.
     */
    MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
    ReduceDriver<Text, IntWritable, Text, DoubleWritable> reduceDriver;
    MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, DoubleWritable> mapReduceDriver;
  
    /*
     * Set up the test. This method will be called before every test.
     */
    @Before
    public void setUp() {

    /*
     * Set up the mapper test harness.
     */
        AverageMapper mapper = new AverageMapper();
        mapDriver = new MapDriver<LongWritable, Text, Text, IntWritable>();
        mapDriver.setMapper(mapper);

    /*
     * Set up the reducer test harness.
     */
        AverageReducer reducer = new AverageReducer();
        reduceDriver = new ReduceDriver<Text, IntWritable, Text, DoubleWritable>();
        reduceDriver.setReducer(reducer);
    
    /*
     * Set up the mapper/reducer test harness.
     */
        mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, DoubleWritable>();
        mapReduceDriver.setMapper(mapper);
        mapReduceDriver.setReducer(reducer);
    }

    /*
     * Test the mapper.
     */
    @Test
    public void testMapper() {

    /*
     * For this test, the mapper's input will be "1 cat cat dog" 
     */
        mapDriver.withInput(new LongWritable(1), new Text("cat cate dog"));
	  
    /*
     * The expected output from mapper phase (given inputs) is "c, 3", "c, 4", and "d, 3".
     */
        mapDriver.withOutput(new Text("c"), new IntWritable(3));
        mapDriver.withOutput(new Text("c"), new IntWritable(4));
        mapDriver.withOutput(new Text("d"), new IntWritable(3));
    
    /*
     * Run the test.
     */
        mapDriver.runTest();
    }

    /*
     * Test the reducer.
     */
    @Test
    public void testReducer() {

        List<IntWritable> values = new ArrayList<IntWritable>();
        values.add(new IntWritable(3));
        values.add(new IntWritable(4));

    /*
     * For this test, the reducer's input will be "c, [3, 4]".
     */
        reduceDriver.withInputKey(new Text("c"));
        reduceDriver.withInputValues(values);

    /*
     * The expected output is "c, 3.5"
     */
        reduceDriver.withOutput(new Text("c"), new DoubleWritable(3.5));

    /*
     * Run the test.
     */
        reduceDriver.runTest();
    }

    /*
     * Test the mapper and reducer working together.
     */
    @Test
    public void testMapReduce() {

    /*
     * For this test, the mapper's input will be "1 cat cat dog" 
     */

        mapReduceDriver.withInput(new LongWritable(1), new Text("cat cate dog"));
	  
    /*
     * The expected output (from the reducer) is "cat 2", "dog 1". 
     */

        mapReduceDriver.withOutput(new Text("c"), new DoubleWritable(3.5));
        mapReduceDriver.withOutput(new Text("d"), new DoubleWritable(3.0));
    /*
     * Run the test.
     */
        mapReduceDriver.runTest();
    }
}