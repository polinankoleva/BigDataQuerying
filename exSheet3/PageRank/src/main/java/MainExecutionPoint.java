import java.util.Arrays;

import org.apache.hadoop.util.ToolRunner;

import ex1a.FriendsOfUserRetrievalDriver;
import ex1b.PageRankDriver;
import ex1c.ExtendedPageRankDriver;

/**
 * 
 * The main execution point of a program. You can run each of the map/reduce
 * tasks by this class. See ReadMe.txt file for better explanation.
 * 
 * @author Polina Koleva
 *
 */
public class MainExecutionPoint {

	public static void main(String[] args) throws Exception {
		// the first argument should be the driver type
		Integer driver = Integer.parseInt(args[0]);
		String[] newArgs = Arrays.copyOfRange(args, 1, args.length);
		switch (driver) {
		case 1:
			int exitCode = ToolRunner.run(new FriendsOfUserRetrievalDriver(),
					newArgs);
			System.exit(exitCode);
			break;
		case 2:
			exitCode = ToolRunner.run(new PageRankDriver(), newArgs);
			System.exit(exitCode);
			break;
		case 3:
			exitCode = ToolRunner.run(new ExtendedPageRankDriver(), newArgs);
			System.exit(exitCode);
			break;
		default:
			System.out
					.println("You should choose one of the specified options. Please, run again with valid parameters. ");
			break;
		}
	}

}
