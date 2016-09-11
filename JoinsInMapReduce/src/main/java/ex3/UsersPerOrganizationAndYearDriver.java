package ex3;

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
 * The main execution point. This driver contains two jobs. The first one
 * extract all needed information per user as the year of graduation and the
 * organization/school name as well as its full name. The second job combines
 * all user per organization and year of graduation. The final result is the
 * pairs of users whose have studied at the same ogranization and graduated at
 * the same time.
 * 
 * @author Polina Koleva
 *
 */
public class UsersPerOrganizationAndYearDriver extends Configured implements
		Tool {

	public int run(String[] args) throws Exception {
		/*
		 * Validate that four arguments which were passed from the command line.
		 */
		if (args.length != 4) {
			System.out
					.printf("Usage: UsersPerOrganizationAndYearDriver <input dir> <temp dir> <output dir> <num of reducers>\n");
			System.exit(-1);
		}

		// Configuration processed by ToolRunner
		Configuration conf = getConf();

		// Notify Hadoop that application uses GenericOptionsParser
		// This is not required but prevents that a warning is printed during
		// execution
		conf.set("mapreduce.client.genericoptionsparser.used", "true");

		// Create a Job using the processed conf
		Job firstJob = Job.getInstance(conf);

		// Define Input and Output Format
		firstJob.setInputFormatClass(TextInputFormat.class);
		firstJob.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(firstJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(firstJob, new Path(args[1]));

		// Define Map Output Classes (Key, Value)
		// We don't have to define this as it is the same as the Job Output.
		// But if it is not the same, you have to define it!
		firstJob.setMapOutputKeyClass(Text.class);
		firstJob.setMapOutputValueClass(Text.class);

		// Define Job Output Classes (Key, Value)
		firstJob.setOutputKeyClass(Text.class);
		firstJob.setOutputValueClass(Text.class);

		// Set Mapper and Reducer Class
		firstJob.setMapperClass(PersonInformationRetrievalMapper.class);
		firstJob.setReducerClass(PersonInformationRetrievalReducer.class);

		// Set the Number of Reduce Tasks
		firstJob.setNumReduceTasks(Integer.parseInt(args[3]));
		System.out.println("Number of reduce tasks:"
				+ Integer.parseInt(args[3]));

		/*
		 * Specify the jar file that contains your driver, mapper, and reducer.
		 * Hadoop will transfer this jar file to nodes in your cluster running
		 * mapper and reducer tasks.
		 */
		firstJob.setJarByClass(UsersPerOrganizationAndYearDriver.class);

		/*
		 * Specify an easily-decipherable name for the job. This job name will
		 * appear in reports and logs.
		 */
		firstJob.setJobName("Person Information Retrieval");
		firstJob.waitForCompletion(true);

		// Create a second job using the processed conf
		Job secondJob = Job.getInstance(conf);

		// Define Input and Output Format
		secondJob.setInputFormatClass(TextInputFormat.class);
		secondJob.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(secondJob, new Path(args[1]));
		FileOutputFormat.setOutputPath(secondJob, new Path(args[2]));

		// Define Map Output Classes (Key, Value)
		// We don't have to define this as it is the same as the Job Output.
		// But if it is not the same, you have to define it!
		secondJob.setMapOutputKeyClass(Text.class);
		secondJob.setMapOutputValueClass(Text.class);

		// Define Job Output Classes (Key, Value)
		secondJob.setOutputKeyClass(Text.class);
		secondJob.setOutputValueClass(Text.class);

		// Set Mapper and Reducer Class
		secondJob.setMapperClass(UsersPerOrganizationAndYearMapper.class);
		secondJob.setReducerClass(UsersPerOrganizationAndYearReducer.class);

		// Set the Number of Reduce Tasks
		secondJob.setNumReduceTasks(Integer.parseInt(args[3]));
		System.out.println("Number of reduce tasks:"
				+ Integer.parseInt(args[3]));

		/*
		 * Specify the jar file that contains your driver, mapper, and reducer.
		 * Hadoop will transfer this jar file to nodes in your cluster running
		 * mapper and reducer tasks.
		 */
		secondJob.setJarByClass(UsersPerOrganizationAndYearDriver.class);

		/*
		 * Specify an easily-decipherable name for the job. This job name will
		 * appear in reports and logs.
		 */
		secondJob.setJobName("CombineUsersPerOrganizationAndYear");

		/*
		 * Start the MapReduce job and wait for it to finish. If it finishes
		 * successfully, return 0. If not, return 1.
		 */
		return secondJob.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new UsersPerOrganizationAndYearDriver(),
				args);
		System.exit(exitCode);
	}
}
