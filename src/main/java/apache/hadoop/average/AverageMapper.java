package apache.hadoop.average;

import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

/**
 * WordCount Mapper
 *
 */


/**
 * This is the WordMapper class from the word count exercise.
 */
public class AverageMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    // Hadoop logging uses log4j
    private static final Logger LOGGER = Logger.getLogger(AverageMapper.class.getName());

    @Override
    public void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper.Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        for (String word : line.split("\\W+")) {
            if (word.length() > 0) {

                // Tip: Concatenation is expensive, make it conditional based on log level.
                if(LOGGER.isDebugEnabled()){
                    LOGGER.debug("Character: " + word.substring(0,1) + ", Length: " + word.length());
                }

                try {

                    context.write(new Text(word.substring(0,1)), new IntWritable(word.length()));

                } catch (Exception e) {

                    LOGGER.error("An exception has occurred in Map phase" + e.toString());
                }
            }
        }
    }
}