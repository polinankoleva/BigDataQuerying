package ex4;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * A mapper class that represents individual task and transforms input records
 * into a intermediate records. It receives a file with statements formatted as
 * <subject predicate object>. It goes over them and search for a specific
 * predicate "foaf:knows". If it is found, the object of the statement will be
 * set as a key value. As a output value will be set 1. The idea is for all
 * ingoing edges of an user, to the reducer to be sent "<user name 1>". For
 * example, if we have a rdf statement "user1 foaf:knows user2" as this is a
 * ingoing edge for user2, the output key will be "user2". Its output value will
 * be 1 representing the number of parsed statements. Finally, the reducer will
 * have a role to count them. The mapper key is text, but the value are
 * intWritable.
 * 
 * 
 * @author Polina Koleva
 *
 */
public class UsersSelectionByPopularityMapper extends
		Mapper<Object, Text, Text, IntWritable> {

	// the searched predicate
	private static final String FRIENDS_RELATIONSHIP = "foaf:knows";
	// variable where the key for the reducer will be stored
	private Text keyText = new Text();
	// variable that will be used to marked the number(1) of each parsed
	// incoming edge per user
	private IntWritable oneIncomingEdge = new IntWritable(1);

	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		// Break statement into object, predicate and subject
		StringTokenizer rdfStatement = new StringTokenizer(value.toString());
		String subject = rdfStatement.nextToken();
		String predicate = rdfStatement.nextToken();
		String object = rdfStatement.nextToken();
		// search for specific predicate
		if (predicate.equals(FRIENDS_RELATIONSHIP)) {
			keyText.set(object);
			context.write(keyText, oneIncomingEdge);
		}

	}
}
