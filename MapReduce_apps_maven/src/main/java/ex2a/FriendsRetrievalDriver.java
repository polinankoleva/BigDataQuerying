package ex2a;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * The main point of the program for retrieval of an user's friends. We
 * determine the input and output folders, the user name of the user for whom
 * the program will be run from the input parameters. Additionally, the
 * configuration of a job , setting of mapper and reducer, their key/values
 * types are done in this class as well. The used mapper class is
 * {@link FriendsRetrievalMapper} and the reducer class is
 * {@link FriendsRetrievalReducer}. For more information see their
 * documentation.
 * 
 * @author Polina Koleva
 *
 */
public class FriendsRetrievalDriver extends Configured implements Tool {

	public static final String USERNAME = "username";

	@Override
	public int run(String[] args) throws Exception {
		/*
		 * Validate that four arguments were passed from the command line.
		 */
		if (args.length != 4) {
			System.out
					.printf("Usage: FriendsRetrievalDriver <input dir> <output dir> <username> <num of reducers>\n");
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

		// parse the username of an user whos friends will be retrieved
		// and add it as a parameter to the job.
		// Therefore, the job's mapper and reducer can see it and use it.
		String username = args[2];
		conf.set(USERNAME, username);

		// Define Input and Output Format
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// set where the input and output folders are
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// Define Map Output Classes (Key, Value)
		// We don't have to define this as it is the same as the Job Output.
		// But if it is not the same, you have to define it!
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// Define Job Output Classes (Key, Value)
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// Set Mapper and Reducer Class
		job.setMapperClass(FriendsRetrievalMapper.class);
		job.setReducerClass(FriendsRetrievalReducer.class);

		// Set the Number of Reduce Tasks
		job.setNumReduceTasks(Integer.parseInt(args[3]));
		System.out.println("Number of reduce tasks:"
				+ Integer.parseInt(args[3]));

		/*
		 * Specify the jar file that contains your driver, mapper, and reducer.
		 * Hadoop will transfer this jar file to nodes in your cluster running
		 * mapper and reducer tasks.
		 */
		job.setJarByClass(FriendsRetrievalDriver.class);

		/*
		 * Specify an easily-decipherable name for the job. This job name will
		 * appear in reports and logs.
		 */
		job.setJobName("FriendsRetrieval");

		/*
		 * Start the MapReduce job and wait for it to finish. If it finishes
		 * successfully, return 0. If not, return 1.
		 */
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new FriendsRetrievalDriver(), args);
		System.exit(exitCode);
	}
}
