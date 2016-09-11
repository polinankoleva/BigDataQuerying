package ex1b;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
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
 * Main execution point. There are two map/reduce phases. The first one contains
 * retrieval of all user's friends. The second one repeats 6 times. It computes
 * the page rank of users.
 * 
 * @author Polina Koleva
 *
 */
public class PageRankDriver extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		/*
		 * Validate that four arguments which were passed from the command line.
		 */
		if (args.length != 4) {
			System.out
					.printf("Usage: PageRankDriver <input dir> <output dir 1> <output dir 2> <num of reducers>\n");
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
		firstJob.setMapperClass(FriendsOfUserRetrievalMapper.class);
		firstJob.setReducerClass(FriendsOfUserRetrievalReducer.class);

		// Set the Number of Reduce Tasks
		firstJob.setNumReduceTasks(Integer.parseInt(args[3]));
		System.out.println("Number of reduce tasks:"
				+ Integer.parseInt(args[3]));

		/*
		 * Specify the jar file that contains your driver, mapper, and reducer.
		 * Hadoop will transfer this jar file to nodes in your cluster running
		 * mapper and reducer tasks.
		 */
		firstJob.setJarByClass(PageRankDriver.class);

		/*
		 * Specify an easily-decipherable name for the job. This job name will
		 * appear in reports and logs.
		 */
		firstJob.setJobName("FriendOfUserRetrieval");

		/*
		 * Start the MapReduce job and wait for it to finish.
		 */
		firstJob.waitForCompletion(true);

		// parse the total number of users - it is used in the next map/reduce
		// phase
		int totalNumberOfUsers = (int) firstJob.getCounters()
				.findCounter(CustomCounters.USERS_COUNT).getValue();
		// having the total number of users, the initial page rank for every
		// user(node)
		// is 1/#users
		float initialPageRank = 1f / totalNumberOfUsers;
		// set it as a parameter in the configuration because we want to use it
		// in the mapper
		conf.setFloat("initialPageRank", initialPageRank);

		boolean jobCompletion = false;
		// For the mapper, we use two folder output folder1 and output folder2.
		// The
		// first job reads its input for the folder1 and writes its output into
		// folder2.
		// The next job uses folder2 as input and folder1 as output folder.
		// After a job finishes, its input folder
		// is deleted(no longer needed). The consecutive job uses folder2 as its
		// input and writes into folder1 and so on.
		// Finally, the result set is found in one of the two output folders.
		// output folder 1
		String firstOutputDirectory = args[1];
		// output folder 2
		String secondOutputDirectory = args[2];
		for (int i = 0; i < 6; i++) {
			String inputDirectory = null;
			String outputDirectory = null;
			if (i % 2 == 0) {
				inputDirectory = firstOutputDirectory;
				outputDirectory = secondOutputDirectory;
			} else {
				inputDirectory = secondOutputDirectory;
				outputDirectory = firstOutputDirectory;
			}

			// the second job that iteratively computes the page rank of each
			// user(node)
			Job secondJob = Job.getInstance(conf);

			FileInputFormat.setInputPaths(secondJob, new Path(inputDirectory));
			FileOutputFormat
					.setOutputPath(secondJob, new Path(outputDirectory));

			// Define Map Output Classes (Key, Value)
			// We don't have to define this as it is the same as the Job Output.
			// But if it is not the same, you have to define it!
			secondJob.setMapOutputKeyClass(Text.class);
			secondJob.setMapOutputValueClass(Text.class);

			// Define Job Output Classes (Key, Value)
			secondJob.setOutputKeyClass(Text.class);
			secondJob.setOutputValueClass(Text.class);

			// Set Mapper and Reducer Class
			secondJob.setMapperClass(PageRankComputationMapper.class);
			secondJob.setReducerClass(PageRankComputationRecuder.class);

			// Set the Number of Reduce Tasks
			secondJob.setNumReduceTasks(Integer.parseInt(args[3]));
			System.out.println("Number of reduce tasks:"
					+ Integer.parseInt(args[3]));

			/*
			 * Specify the jar file that contains your driver, mapper, and
			 * reducer. Hadoop will transfer this jar file to nodes in your
			 * cluster running mapper and reducer tasks.
			 */
			secondJob.setJarByClass(PageRankDriver.class);

			/*
			 * Specify an easily-decipherable name for the job. This job name
			 * will appear in reports and logs.
			 */
			secondJob.setJobName("PageRankComputation");

			/*
			 * Start the MapReduce job and wait for it to finish.
			 */
			jobCompletion = secondJob.waitForCompletion(true);
			deleteDirectory(inputDirectory, conf);
		}
		return jobCompletion ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new PageRankDriver(), args);
		System.exit(exitCode);
	}

	// just delete a directory from the file system
	public void deleteDirectory(String path, Configuration conf)
			throws IOException {
		// Delete output if exists
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(new Path(path))) {
			hdfs.delete(new Path(path), true);
		}
	}
}
