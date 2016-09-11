package ex4;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * A mapper class that represents individual task and transforms input records
 * into a intermediate records. It goes over all rdf statements, parses them and
 * searches for specific predicate "foaf:knows". When it is found and depending
 * on source index of the read data set there are two options: either the
 * composite key will be <<object,0> rdf statement> or <<subject,1> rdf
 * statement> where the notation means <<join key, sourceIndex> value>. It
 * prepares the data for the reduce phase where a join of the data set with
 * itself will be done. The idea is that if we have a rdf statement from the
 * first data set <user1 foaf:knows user2> and rdf statements from the second
 * data set "<user2 foaf:knows user3> , <user2 foaf:knows user4>", they will be
 * sent to one reducer where as for "user1", "user3" and "user4" can be set as
 * friend of its friends, because of the relationship between "user1" and
 * "user2". We filter the data from the first data set by given user which the
 * path has to start with.
 * 
 * @author Polina Koleva
 *
 */
public class FirstPhaseFoafPathFinderMapper extends
		Mapper<Object, Text, CompositeKeyWritable, Text> {

	// the searched predicate
	private final String FRIENDS_RELATIONSHIP = "foaf:knows";
	// a variable where composite key will be stored
	private CompositeKeyWritable compositeKey = new CompositeKeyWritable();
	// the value out that will be sent to the reducer
	private Text statementText = new Text();

	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		// parse the searched user from which a path has to start
		String fromUser = context.getConfiguration().get("fromUser");
		// Break rdf statement into subject, predicate, object
		StringTokenizer rdfStatement = new StringTokenizer(value.toString());
		String subject = rdfStatement.nextToken();
		String predicate = rdfStatement.nextToken();
		String object = rdfStatement.nextToken();
		// if the edge represents "is-a-friend-of"
		if (predicate.equals(FRIENDS_RELATIONSHIP)) {
			// remove the all unneeded characters
			String valueFinal = value.toString().replace(".", "").trim();
			statementText.set(valueFinal);
			// send to the reducer all tuples <firstUserAtPath foaf:knows userN>
			// they will be as our first data set
			if (fromUser.equals(subject)) {
				compositeKey.setJoinKey(object);
				compositeKey.setSourceIndex(0);
				context.write(compositeKey, statementText);
			}
			// send each tuple <userN foaf:knows userM> with key userN
			// these tuples will be as our second data set
			// the idea is to receive in the reducer <firstUserAtPath foaf:knows
			// userN><userN foaf:knows userM>
			// all triples reached a reducer where there is no information from
			// the first data set
			// will be ignored
			compositeKey.setJoinKey(subject);
			compositeKey.setSourceIndex(1);
			context.write(compositeKey, statementText);

		}
	}
}
