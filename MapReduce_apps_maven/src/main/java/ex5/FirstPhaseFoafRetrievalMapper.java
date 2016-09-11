package ex5;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * A mapper class that represents individual task and transforms input records
 * into a intermediate records. It goes over all rdf statements, parse them and
 * search for specific predicate "foaf:knows". When it is found and depending on
 * source index of the read data set there are two options: either the composite
 * key will be <<object,0> rdf statement> or <<subject,2> rdf statement> where
 * the notation means <<join key, sourceIndex> value>. This map will be used to
 * read the same data set twice. It prepares the data for the reduce phase where
 * a join of the dataset with itself will be done. The idea is that if we have a
 * rdf statement from the first data set <user1 foaf:knows user2> and rdf
 * statements from the second data set
 * "<user2 foaf:knows user3> , <user2 foaf:knows user4>", they will be sent to
 * one reducer where as for "user1", "user3" and "user4" can be set as friend of
 * its friends, because of the relationship between "user1" and "user2".
 * 
 * @author Polina Koleva
 *
 */
public class FirstPhaseFoafRetrievalMapper extends
		Mapper<Object, Text, CompositeKeyWritable, Text> {

	// the searched predicate
	private static final String FRIENDS_RELATIONSHIP = "foaf:knows";
	// a variable where composite key will be stored
	private CompositeKeyWritable compositeKey = new CompositeKeyWritable();
	// a variable representing the source index for the data set which is read
	private static int sourceIndex = 0;
	// the value out that will be sent to the reducer
	private Text statementText = new Text();

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		// change the source index depending on times when the mapper is invoked
		// because this mapper will be used for reading the same data set twice
		// and the join will be over this data set with itself
		// The first time when the data set is read, the sourceIndex will be 1
		// the second time, it will be 0.
		if (sourceIndex == 0) {
			sourceIndex++;
		} else {
			sourceIndex--;
		}
	}

	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		// Break rdf statement into subject, predicate, object
		StringTokenizer rdfStatement = new StringTokenizer(value.toString());
		String subject = rdfStatement.nextToken();
		String predicate = rdfStatement.nextToken();
		String object = rdfStatement.nextToken();
		// if the edge represents "is-a-friend-of"
		if (predicate.equals(FRIENDS_RELATIONSHIP)) {
			// just remove the predicate it is no longer needed
			// send only subject object
			statementText.set(subject + " " + object);
			// we join all "foaf:knows" entries with itself. Once, we assume
			// that a "foaf:knows" entry comes from "the first table"
			// in the join (sourceIndex = 0), once the opposite - it comes from
			// the "second table" in the join (sourceIndex = 1)
			// For example, if we assume that we are processing the entries from
			// the "first table" in the join and we have a statement
			// "user1 foaf:knows user2". The join key will be "user2" and
			// sourceIndex - 0. The former is "user2" because we want to
			// search for all friends of this user.
			// On the other hand, if we are mapping over the "second table" (in
			// our case the same). the sourceIndex will be 1. Moreover,
			// if we have an entry "user2 foaf:knows user3", the join key will
			// be "user2". Therefore, the full list of friends of "user2"
			// will be available in the reducer.
			if (sourceIndex == 0) {
				compositeKey.setJoinKey(object);
			} else {
				compositeKey.setJoinKey(subject);
			}
			compositeKey.setSourceIndex(sourceIndex);
			context.write(compositeKey, statementText);
		}
	}
}
