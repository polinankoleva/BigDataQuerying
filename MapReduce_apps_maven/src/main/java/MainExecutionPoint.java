import java.util.Arrays;

import org.apache.hadoop.util.ToolRunner;

import ex2a.FriendsRetrievalDriver;
import ex2b.LikeCountDriver;
import ex3.AverageLikeCountDriver;
import ex4.UsersSelectionByPopularityDriver;
import ex5.FoafRetrievalDriver;

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
		case 0:
			int exitCode = ToolRunner
					.run(new FriendsRetrievalDriver(), newArgs);
			System.exit(exitCode);
			break;
		case 1:
			exitCode = ToolRunner.run(new LikeCountDriver(), newArgs);
			System.exit(exitCode);
			break;
		case 2:
			exitCode = ToolRunner.run(new AverageLikeCountDriver(), newArgs);
			System.exit(exitCode);
			break;
		case 3:
			exitCode = ToolRunner.run(new UsersSelectionByPopularityDriver(),
					newArgs);
			System.exit(exitCode);
			break;
		case 4:
			exitCode = ToolRunner.run(new FoafRetrievalDriver(), newArgs);
			System.exit(exitCode);
			break;
		default:
			System.out
					.println("You should choose one of the specified option. Please, run again with valid parameter. ");
			break;
		}
	}

}
