package ex3;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * The second reducer. One reducer per organization and year of graduation.
 * Information for each user graduated an organization in particular year will
 * reach one reducer. The information for all such user is combined and written
 * to the output file.
 * 
 * @author Polina Koleva
 *
 */
public class UsersPerOrganizationAndYearReducer extends
		Reducer<Text, Text, Text, Text> {

	private Text valueOut = new Text();

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		StringBuilder sb = new StringBuilder();
		Iterator<Text> it = values.iterator();
		int usersCount = 0;
		while (it.hasNext()) {
			usersCount++;
			// just concatenate for all users (userId, fullName) into one string
			sb.append(it.next() + " ");
		}
		// check if we have more than one user per organization and year
		// add to the result set - only if the users are two or more
		if (usersCount >= 2) {
			valueOut.set(sb.toString());
			context.write(key, valueOut);
		}

	}
}
