package ex2a;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * A mapper class that represents individual task and transforms input records
 * into a intermediate records. It goes over all rdf statements, parse them and
 * search for specific predicate "foaf:knows". When it is found, the subject of
 * already parsed rdf statement is set as a key and its object as a value. The
 * idea is collecting of all friends in one reducer. For example, if the mapper
 * encounters statements "user1 foaf:knows user2" and "user1 foaf:knows user3",
 * for both of them as a key will be set "user1". Therefore , they will reach
 * the same reducer where all friends of "user1" can be retrieved because for
 * all of them "user1" will be set as a key value. The mapper key and value are
 * text. The output of a mapper will be <subject object> for all rdf statements
 * which have a predicate "foaf:knows".
 * 
 * @author Polina Koleva
 *
 */
public class FriendsRetrievalMapper extends Mapper<Object, Text, Text, Text> {

	// the searched predicate
	private static final String FRIENDS_RELATIONSHIP = "foaf:knows";
	// a variable where the subject of parsed rdf statement will be saved. It
	// will be used as a key.
	private Text user = new Text();
	// this variable will be used as a value for the reducer
	private Text friend = new Text();

	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		// get the username for whom we need to retrieve all his/her friends
		String username = context.getConfiguration().get(
				FriendsRetrievalDriver.USERNAME);
		// Break rdf statement into object, predicate, subject
		StringTokenizer rdfStatement = new StringTokenizer(value.toString());
		String subject = rdfStatement.nextToken();
		String predicate = rdfStatement.nextToken();
		String object = rdfStatement.nextToken();
		// check if the "subject" is the same user that we need
		// and check if the "subject" and the "object" are friends
		// check their relationship - represented by the edge 'foaf:knows'
		if (subject.equals(username) && predicate.equals(FRIENDS_RELATIONSHIP)) {
			// set subject as a key thus the friends of an user will reach the
			// same reducer
			user.set(subject);
			friend.set(object);
			context.write(user, friend);
		}
	}
}
