package ex1c;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * One reducer per each user. It formats all friends of an user in a list as
 * (user1, user2, ....). The output is just a user mapped to a list of its
 * friends. Besides, to one specific reducer, all users will be sent. It is
 * responsible for counting their total number and send it to the driver class.
 * This information is used in the next map/reduce phase.
 * 
 * @author Polina Koleva
 *
 */
public class FriendsOfUserRetrievalReducer extends
		Reducer<Text, Text, Text, Text> {

	// the key for the reducer where the counting of total number of users is
	// done
	private final String USER_COUNT_REDUCER_KEY = "userNodesCountReducer";
	private Text usersCountReducerKey = new Text(USER_COUNT_REDUCER_KEY);

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// if the incoming key is the same as for the reducer where counting is
		// done
		if (key.equals(usersCountReducerKey)) {
			// stores all distinct users in a list
			ArrayList<String> distinctUsers = new ArrayList<String>();
			Iterator<Text> iterator = values.iterator();
			int usersCount = 0;
			while (iterator.hasNext()) {
				Text user = iterator.next();
				// if an user is not in "our distinct users" list
				// add it and increment the counter for total number of users
				// otherwise - just continue to the next user
				if (!distinctUsers.contains(user.toString())) {
					distinctUsers.add(user.toString());
					usersCount++;
				}
			}
			// stores the total number of users in a counter
			// driver, other mappers and reducers can read this value
			context.getCounter(CustomCounters.USERS_COUNT).setValue(usersCount);
		} else {
			// just format all friends of an user - (user2, user3, user4,...)
			String outputValue = formatOutputValue(values);
			// set as key userId
			context.write(key, new Text(outputValue));
		}
	}

	// format data as the output pattern - (user1, user2, user3, ..)
	public String formatOutputValue(Iterable<Text> values) {
		StringBuilder sb = new StringBuilder();
		sb.append("(");
		Iterator<Text> iterator = values.iterator();
		if (!iterator.hasNext()) {
			return null;
		}
		while (iterator.hasNext()) {
			Text next = iterator.next();
			if (!iterator.hasNext()) {
				sb.append(next.toString());
			} else {
				sb.append(next.toString() + ", ");
			}
		}
		sb.append(")");
		return sb.toString();
	}
}
