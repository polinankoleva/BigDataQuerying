package ex4;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * The main execution point. It contains two or more consecutive jobs. The first
 * job's mapper is {@link FirstPhaseFoafPathFinderMapper}, the reducer is
 * {@link FirstPhaseFoafPathFinderReducer}. The second(and if needed more jobs)
 * job has two mappers - {@link SecondPhaseFirstFoafPathFinderMapper} and
 * {@link SecondPhaseSecondFoafPathFinderMapper} and one Reducer
 * {@link SecondPhaseFoafPathFinderReducer}. It finds paths from one user to
 * another which are specified by input parameters.
 * 
 * @author Polina Koleva
 *
 */
public class FoafPathFinderDriver extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		/*
		 * Validate that six arguments were passed from the command line.
		 */
		if (args.length != 6) {
			System.out
					.printf("Usage: FriendsOfTheirFriendsRetrieval <fromUser> <toUser> <input dir> <output1 dir> <output2 dir> <num of reducers>\n");
			System.exit(-1);
		}

		// Configuration processed by ToolRunner
		Configuration conf = getConf();

		// Notify Hadoop that application uses GenericOptionsParser
		// This is not required but prevents that a warning is printed during
		// execution
		conf.set("mapreduce.client.genericoptionsparser.used", "true");

		// set start and end user of searched paths <fromUser foaf:knows ...
		// foaf:knows toUser>
		conf.set("fromUser", args[0]);
		conf.set("toUser", args[1]);

		// Create a Job using the processed conf
		Job firstJob = Job.getInstance(conf);

		// Define Input and Output Format
		firstJob.setInputFormatClass(TextInputFormat.class);
		firstJob.setOutputFormatClass(TextOutputFormat.class);

		// input directories
		// the directory with the social graph file
		Path inputPath = new Path(args[2]);
		// the output directory for the first job - it will be used as a input
		// directory for the others jobs
		Path firstOutputPath = new Path(args[3]);
		FileInputFormat.setInputPaths(firstJob, inputPath);
		FileOutputFormat.setOutputPath(firstJob, firstOutputPath);

		// two output files - one for paths which are of type <fromUser
		// foaf:knows ... foaf:knows toUser>
		// and one for temporary paths which need extension/adding of edges to
		// reach the toUser (if possible)
		MultipleOutputs.addNamedOutput(firstJob, "finalPaths",
				TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(firstJob, "temporaryPaths",
				TextOutputFormat.class, Text.class, Text.class);

		// Define Map Output Classes (Key, Value)
		// We don't have to define this as it is the same as the Job Output.
		// But if it is not the same, you have to define it!
		firstJob.setMapOutputKeyClass(CompositeKeyWritable.class);
		firstJob.setMapOutputValueClass(Text.class);

		// set a custom partitioner that will take into account only the join
		// attribute
		firstJob.setPartitionerClass(FoafPartitioner.class);
		// set how the data to be ordered
		firstJob.setSortComparatorClass(FoafSortingComparator.class);
		// set a custom class that will be used for grouping within the reducer
		firstJob.setGroupingComparatorClass(FoafGroupingComparator.class);

		// Define Job Output Classes (Key, Value)
		firstJob.setOutputKeyClass(Text.class);
		firstJob.setOutputValueClass(Text.class);

		// Set Mapper and Reducer Class
		firstJob.setMapperClass(FirstPhaseFoafPathFinderMapper.class);
		firstJob.setReducerClass(FirstPhaseFoafPathFinderReducer.class);

		// Set the Number of Reduce Tasks
		firstJob.setNumReduceTasks(Integer.parseInt(args[5]));
		System.out.println("Number of reduce tasks:"
				+ Integer.parseInt(args[5]));

		/*
		 * Specify the jar file that contains your driver, mapper, and reducer.
		 * Hadoop will transfer this jar file to nodes in your cluster running
		 * mapper and reducer tasks.
		 */
		firstJob.setJarByClass(FoafPathFinderDriver.class);

		/*
		 * Specify an easily-decipherable name for the job. This job name will
		 * appear in reports and logs.
		 */
		firstJob.setJobName("Foaf path finder - first phase");
		// wait until the first job finishes
		boolean jobCompletion = firstJob.waitForCompletion(true);

		// the first job builds paths like <fromUser foaf:knows user1 foaf:knows
		// user2 2>
		// and joins the initial social graph with itself
		// the second and consecutive jobs(if they are needed) try to extend
		// already found from
		// the previous job paths with one more edge. They consist of two
		// mappers and one reducer.
		// The first mapper processes data from the social graph. Therefore, it
		// uses only the initial input folder.
		// For the second mapper, we use two folder folder1 and folder2. The
		// first job writes its output into folder1.
		// The next job uses folder1 as input and folder2 as output folder.
		// After it finishes, its input folder
		// is deleted(no longer needed). The consecutive job uses folder2 as its
		// input and writes into folder1 and so on.
		// Finally, the result set is found in one of the two output folders.
		String firstOutputDirectory = args[3];
		String secondOutputDirectory = args[4];
		if (!isFinalPathFound(firstOutputDirectory)) {
			// we search for max. distance path 10
			// each job extends the path with one edge and we start with paths
			// which distance is 2
			for (int i = 0; i < 8; i++) {
				String temporaryInputDirectory = null;
				String outputDirectory = null;
				if (i % 2 == 0) {
					temporaryInputDirectory = firstOutputDirectory;
					outputDirectory = secondOutputDirectory;
				} else {
					temporaryInputDirectory = secondOutputDirectory;
					outputDirectory = firstOutputDirectory;
				}
				// Create a Job using the processed conf
				Job secondJob = Job.getInstance(conf);

				// input directories
				// set the specific mapper for each input file
				MultipleInputs.addInputPath(secondJob, inputPath,
						TextInputFormat.class,
						SecondPhaseFirstFoafPathFinderMapper.class);
				MultipleInputs.addInputPath(secondJob, new Path(
						temporaryInputDirectory), TextInputFormat.class,
						SecondPhaseSecondFoafPathFinderMapper.class);
				// output directory
				FileOutputFormat.setOutputPath(secondJob, new Path(
						outputDirectory));

				// two output files - one for found final paths
				// and one for temporary paths
				MultipleOutputs.addNamedOutput(secondJob, "finalPaths",
						TextOutputFormat.class, Text.class, Text.class);
				MultipleOutputs.addNamedOutput(secondJob, "temporaryPaths",
						TextOutputFormat.class, Text.class, Text.class);

				// Define Map Output Classes (Key, Value)
				// We don't have to define this as it is the same as the Job
				// Output.
				// But if it is not the same, you have to define it!
				secondJob.setMapOutputKeyClass(CompositeKeyWritable.class);
				secondJob.setMapOutputValueClass(Text.class);

				// set a custom partitioner that will take into account only the
				// join attribute
				secondJob.setPartitionerClass(FoafPartitioner.class);
				// set how the data to be ordered
				secondJob.setSortComparatorClass(FoafSortingComparator.class);
				// set a custom class that will be used for grouping within the
				secondJob
						.setGroupingComparatorClass(FoafGroupingComparator.class);

				// Define Job Output Classes (Key, Value)
				secondJob.setOutputKeyClass(Text.class);
				secondJob.setOutputValueClass(Text.class);

				// set Reducer Class, we have already specify the two mappers
				secondJob
						.setReducerClass(SecondPhaseFoafPathFinderReducer.class);

				// Set the Number of Reduce Tasks
				secondJob.setNumReduceTasks(Integer.parseInt(args[5]));
				System.out.println("Number of reduce tasks:"
						+ Integer.parseInt(args[5]));

				/*
				 * Specify the jar file that contains your driver, mapper, and
				 * reducer. Hadoop will transfer this jar file to nodes in your
				 * cluster running mapper and reducer tasks.
				 */
				secondJob.setJarByClass(FoafPathFinderDriver.class);

				/*
				 * Specify an easily-decipherable name for the job. This job
				 * name will appear in reports and logs.
				 */
				secondJob.setJobName("Foaf Path Finder - Second Phase");

				/*
				 * Start the MapReduce job and wait for it to finish.
				 */
				jobCompletion = secondJob.waitForCompletion(true);
				deleteDirectory(temporaryInputDirectory, conf);
				// if we have already found a path <fromUser foaf:knows ...
				// foaf:knows toUser>,
				// a file with name "finalPaths-sth" is produced, so we can stop
				// searching
				if (isFinalPathFound(outputDirectory)) {
					break;
				}
			}
		}

		return jobCompletion ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new FoafPathFinderDriver(), args);
		System.exit(exitCode);
	}

	// checks if there is a file which name starts with "finalPaths".
	// If true, we have already found a path/paths <fromUser foaf:knows ...
	// foaf:knows toUser>
	// we can stop searching
	public boolean isFinalPathFound(String outputDirectory) {
		File folder = new File(outputDirectory);
		File[] listOfFiles = folder.listFiles();
		for (int i = 0; i < listOfFiles.length; i++) {
			if (listOfFiles[i].getName().startsWith("finalPaths")) {
				return true;
			}
		}
		return false;
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
