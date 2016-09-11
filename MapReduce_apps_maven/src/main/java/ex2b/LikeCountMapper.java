package ex2b;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * A mapper class that represents individual task and transforms input records
 * into a intermediate records. It goes over all rdf statements, parse them and
 * search for specific predicate "fsib:like". When it is found, the subject of
 * already parsed rdf statement is set as a key and as a value - one which
 * represents the like. The idea is for all likes that the user did, to the
 * reducer to be sent "<user name 1>".Finally, the reducer will have a role to
 * count them. The mapper key is text, but the value are intWritable. 
 * 
 * @author Polina Koleva
 *
 */
public class LikeCountMapper extends Mapper<Object, Text, Text, IntWritable> {

	// variable used as a key where the username or subject of parsed rdf
	// statement will be stored
	private Text username = new Text();
	// number of likes per parse rdf statement "subject sib:like object"
	private final static IntWritable one = new IntWritable(1);
	// the specific predicate we will be searching for
	private static final String LIKE_RELATIONSHIP = "sib:like";

	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		// Break rdf statement into object, predicate and subject
		StringTokenizer rdfdStatement = new StringTokenizer(value.toString());
		String subject = rdfdStatement.nextToken();
		String predicate = rdfdStatement.nextToken();
		String object = rdfdStatement.nextToken();
		// if the edge represents "like"
		if (predicate.equals(LIKE_RELATIONSHIP)) {
			// add the username of the user and a value one that represents this
			// like
			username.set(subject);
			context.write(username, one);
		}
	}
}
