package apache.hadoop.average;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * This is the SumReducer class from the word count exercise
 */
public class AverageReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        double avgSum = 0;
        double numValues = 0;

        for (IntWritable value : values) {
            avgSum += value.get();
            numValues++;
        }
        double avg = avgSum / numValues;
        context.write(key, new DoubleWritable(avg));
    }
}