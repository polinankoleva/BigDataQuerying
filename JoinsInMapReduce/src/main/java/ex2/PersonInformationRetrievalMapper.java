package ex2;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * The mapper extracts the searched information for each user and sends it to a
 * reducer. As a key, it sets a userId - one reducer per user.
 * 
 * @author Polina Koleva
 *
 */
public class PersonInformationRetrievalMapper extends
		Mapper<Object, Text, Text, Text> {

	// the searched predicates
	private final String PREDICATE_FIRST_NAME = "foaf:firstName";
	private final String PREDICATE_LAST_NAME = "foaf:lastName";
	private final String PREDICATE_BIRTHDAY = "foaf:birthday";
	private final String PREDICATE_GENDER = "foaf:gender";

	// variable where the key for the reducer will be stored
	private Text username = new Text();

	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		// Break statement into object, predicate and subject
		String[] rdfParts = value.toString().split(
				" (?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
		String subject = rdfParts[0];
		String predicate = rdfParts[1];
		// search for specific predicates and send them to the reducer
		// for each user - one reducer
		if (predicate.equals(PREDICATE_FIRST_NAME)
				|| predicate.equals(PREDICATE_LAST_NAME)
				|| predicate.equals(PREDICATE_BIRTHDAY)
				|| predicate.equals(PREDICATE_GENDER)) {
			username.set(subject);
			context.write(username, value);
		}
	}
}
