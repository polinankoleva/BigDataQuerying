import java.util.Arrays;

import org.apache.hadoop.util.ToolRunner;

import ex1.NumberOfPostsPerUserDriver;
import ex2.PersonInformationRetrievalDriver;
import ex3.UsersPerOrganizationAndYearDriver;
import ex4.FoafPathFinderDriver;

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
			int exitCode = ToolRunner
					.run(new NumberOfPostsPerUserDriver(), newArgs);
			System.exit(exitCode);
			break;
		case 2:
			exitCode = ToolRunner.run(new PersonInformationRetrievalDriver(), newArgs);
			System.exit(exitCode);
			break;
		case 3:
			exitCode = ToolRunner.run(new UsersPerOrganizationAndYearDriver(), newArgs);
			System.exit(exitCode);
			break;
		case 4:
			exitCode = ToolRunner.run(new FoafPathFinderDriver(),
					newArgs);
			System.exit(exitCode);
			break;
		default:
			System.out
					.println("You should choose one of the specified options. Please, run again with valid parameter. ");
			break;
		}
	}

}
