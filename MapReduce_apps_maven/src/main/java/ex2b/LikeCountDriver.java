package ex2b;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * The main point of the program for counting the number of likes for each user.
 * We determine the input and output folders from the input parameters.
 * Additionally, the configuration of a job , setting of mapper and reducer,
 * their key/values types are done in this class as well. The used mapper class
 * is {@link LikeCountMapper} and the reducer class is {@link LikeCountReducer}.
 * For more information see their documentation.
 * 
 * @author Polina Koleva
 *
 */
public class LikeCountDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		/*
		 * Validate that three arguments were passed from the command line.
		 */
		if (args.length != 3) {
			System.out
					.printf("Usage: LikeCountDriver <input dir> <output dir> <num of reducers>\n");
			System.exit(-1);
		}

		// Configuration processed by ToolRunner
		Configuration conf = getConf();

		// Notify Hadoop that application uses GenericOptionsParser
		// This is not required but prevents that a warning is printed during
		// execution
		conf.set("mapreduce.client.genericoptionsparser.used", "true");

		// Create a Job using the processed conf
		Job job = Job.getInstance(conf);

		// Define Input and Output Format
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// Define Map Output Classes (Key, Value)
		// We don't have to define this as it is the same as the Job Output.
		// But if it is not the same, you have to define it!
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		// Define Job Output Classes (Key, Value)
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// Set Mapper and Reducer Class
		job.setMapperClass(LikeCountMapper.class);
		job.setReducerClass(LikeCountReducer.class);

		// For LikeCountDriver we can use the Reducer class also as Combiner
		// class
		// do the same as reducer but on the local level
		// so finally we will do the reduce step twice
		job.setCombinerClass(LikeCountReducer.class);

		// Set the Number of Reduce Tasks
		job.setNumReduceTasks(Integer.parseInt(args[2]));
		System.out.println("Number of reduce tasks:"
				+ Integer.parseInt(args[2]));

		/*
		 * Specify the jar file that contains your driver, mapper, and reducer.
		 * Hadoop will transfer this jar file to nodes in your cluster running
		 * mapper and reducer tasks.
		 */
		job.setJarByClass(LikeCountDriver.class);

		/*
		 * Specify an easily-decipherable name for the job. This job name will
		 * appear in reports and logs.
		 */
		job.setJobName("LikeCount");

		/*
		 * Start the MapReduce job and wait for it to finish. If it finishes
		 * successfully, return 0. If not, return 1.
		 */
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new LikeCountDriver(), args);
		System.exit(exitCode);
	}
}
