package ex4;

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
 * The main point of the program for retrieving of users who have more friends
 * that specific threshold. We determine the input and output folders, the
 * threshold for friends from the input parameters. The latter will be used for
 * selection. If an user is not known by more or equal number of users as the
 * given threshold, it won't be selected as a part of the final result that the
 * job produces. Additionally, the configuration of a job , setting of mapper
 * and reducer, their key/values types are done in this class as well. The used
 * mapper class is {@link UsersSelectionByPopularityMapper} and the reducer
 * class is {@link UsersSelectionByPopularityReducer}. For more information see
 * their documentation.
 * 
 * @author Polina Koleva
 *
 */
public class UsersSelectionByPopularityDriver extends Configured implements
		Tool {

	// the name that will be used as a key when the value of popularity
	// threshold
	// is set to the configuration object
	public static final String popularityThreshold = "popularityThreshold";

	@Override
	public int run(String[] args) throws Exception {
		/*
		 * Validate that four arguments were passed from the command line.
		 */
		if (args.length != 4) {
			System.out
					.printf("Usage: UsersSelectionByPopularityDriver <input dir> <output dir> <popularity threshold> <num of reducers>\n");
			System.exit(-1);
		}

		// Configuration processed by ToolRunner
		Configuration conf = getConf();

		// Notify Hadoop that application uses GenericOptionsParser
		// This is not required but prevents that a warning is printed during
		// execution
		conf.set("mapreduce.client.genericoptionsparser.used", "true");

		// parse the popularity threshold and add it as a parameter to the job.
		// Therefore, the job's mapper and reducer can see it and use it.
		int popularityThreshold = Integer.parseInt(args[2]);
		conf.setInt(UsersSelectionByPopularityDriver.popularityThreshold,
				popularityThreshold);

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
		job.setMapperClass(UsersSelectionByPopularityMapper.class);
		job.setReducerClass(UsersSelectionByPopularityReducer.class);
		// For UsersSelectionByPopularity we can use the Reducer class also as
		// Combiner class
		// it does the same as the reducer but on the local level
		// so finally we will do the reduce step twice
		job.setCombinerClass(UsersSelectionByPopularityReducer.class);

		// Set the Number of Reduce Tasks
		job.setNumReduceTasks(Integer.parseInt(args[3]));
		System.out.println("Number of reduce tasks:"
				+ Integer.parseInt(args[3]));

		/*
		 * Specify the jar file that contains your driver, mapper, and reducer.
		 * Hadoop will transfer this jar file to nodes in your cluster running
		 * mapper and reducer tasks.
		 */
		job.setJarByClass(UsersSelectionByPopularityDriver.class);

		/*
		 * Specify an easily-decipherable name for the job. This job name will
		 * appear in reports and logs.
		 */
		job.setJobName("UsersSelectionByPopularity");

		/*
		 * Start the MapReduce job and wait for it to finish. If it finishes
		 * successfully, return 0. If not, return 1.
		 */
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new UsersSelectionByPopularityDriver(),
				args);
		System.exit(exitCode);
	}
}
