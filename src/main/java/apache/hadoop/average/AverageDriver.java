package apache.hadoop.average;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

/* 
 * MapReduce jobs are typically implemented by using a driver class.
 * The purpose of a driver class is to set up the configuration for the
 * MapReduce job and to run the job.
 * Typical requirements for a driver class include configuring the input
 * and output data formats, configuring the map and reduce classes,
 * and specifying intermediate data formats.
 * 
 * The following is the code for the driver class:
 *
 * NOTE: DE - Added 'ToolRunner' that allows me to dynamically configure job using parameters
 * entered at runtime.  I can use this feature to add Partioner's, Combiners, or set the numReducers at
 * the time the job is executed instead of having to specify in the code itself.  Useful feature for testing many
 * different options quickly.  When running use -D <option_name> on the command line.  Note: job.xml file contains a
 * list of all parameters that can be set using this method (accessible through the job tracker web ui).
 */
public class AverageDriver extends Configured implements Tool{

  public int run(String[] args) throws Exception {

    /*
     * Instantiate a Job object for your job's configuration.  
     */
    Job job = new Job(getConf());
    
    /*
     * Specify the jar file that contains your driver, mapper, and reducer.
     * Hadoop will transfer this jar file to nodes in your cluster running
     * mapper and reducer tasks.
     */
    job.setJarByClass(AverageDriver.class);
    
    /*
     * Specify an easily-decipherable name for the job.
     * This job name will appear in reports and logs.
     */
    job.setJobName("Average");

    /*
     * Specify the paths to the input and output data based on the
     * command-line arguments.
     */
    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    /*
     * Specify the mapper and reducer classes.
     */
    job.setMapperClass(AverageMapper.class);
    job.setReducerClass(AverageReducer.class);

    /*
     * For the word count application, the input file and output 
     * files are in text format - the default format.
     * 
     * In text format files, each record is a line delineated by a 
     * by a line terminator.
     * 
     * When you use other input formats, you must call the 
     * SetInputFormatClass method. When you use other 
     * output formats, you must call the setOutputFormatClass method.
     */
      
    /*
     * For the word count application, the mapper's output keys and
     * values have the same data types as the reducer's output keys 
     * and values: Text and IntWritable.
     * 
     * When they are not the same data types, you must call the 
     * setMapOutputKeyClass and setMapOutputValueClass 
     * methods.
     */

    /*
     * Specify the job's output key and value classes.
     */
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);

    job.setNumReduceTasks(1);

    /*
     * Start the MapReduce job and wait for it to finish.
     * If it finishes successfully, return 0. If not, return 1.
     */
    boolean success = job.waitForCompletion(true);
    return success ? 0 : 1;
  }

    /*
 * The main method calls the ToolRunner.run method, which
 * calls an options parser that interprets Hadoop command-line
 * options and puts them into a Configuration object.
 */
    public static void main(String[] args) throws Exception {

    /*
     * The expected command-line arguments are the paths containing
     * input and output data. Terminate the job if the number of
     * command-line arguments is not exactly 2.
     */
        if (args.length < 2) {
            System.out.printf(
                    "Usage: WordCount <input dir> <output dir>%n");
            System.exit(-1);
        }

        int exitCode = ToolRunner.run(new Configuration(), new AverageDriver(), args);
        System.exit(exitCode);
    }

}
