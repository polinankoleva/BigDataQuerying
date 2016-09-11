package ex1c;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * The mapper extracts friends of an user. As a key, it sets a userId - one
 * reducer per user.
 * 
 * @author Polina Koleva
 *
 */
public class FriendsOfUserRetrievalMapper extends
		Mapper<Object, Text, Text, Text> {

	// the searched predicates
	private final String PREDICATE_FRIENDSHIP = "foaf:knows";
	// this key is used for counting the total number of users from which the
	// graph is constructed
	// each user in a relationship "foaf:knows" is sent to one reducer(using
	// this key)
	// where their total number is counted
	private final String USER_COUNT_REDUCER_KEY = "userNodesCountReducer";

	// variable where the key for the reducer will be stored
	private Text userId = new Text();
	private Text userFriend = new Text();
	private Text usersCountReducerKey = new Text(USER_COUNT_REDUCER_KEY);

	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		// Break statement into object, predicate and subject
		String[] rdfParts = value.toString().split(
				" (?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
		String subject = rdfParts[0];
		String predicate = rdfParts[1];
		String object = rdfParts[2];
		// search for specific predicates by which a user is connected to its
		// friends
		// for each user - one reducer
		if (predicate.equals(PREDICATE_FRIENDSHIP)) {
			// set userId as a key
			userId.set(subject);
			// set a friend of the user as a value
			userFriend.set(object);
			context.write(userId, userFriend);

			// each user in the relationship "foaf:knows" is sent
			// to a reducer where users' total number is counted
			context.write(usersCountReducerKey, userId);
		}
	}
}
