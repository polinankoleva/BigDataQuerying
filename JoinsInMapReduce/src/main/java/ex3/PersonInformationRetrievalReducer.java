package ex3;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * The first reducer.One reducer per each user. It extracts needed user's
 * information, formats ii and writes it to the output file.
 * 
 * @author Polina Koleva
 *
 */
public class PersonInformationRetrievalReducer extends
		Reducer<Text, Text, Text, Text> {

	// the searched predicates
	private final String PREDICATE_FIRST_NAME = "foaf:firstName";
	private final String PREDICATE_LAST_NAME = "foaf:lastName";
	private final String PREDICATE_ORGANIZATION = "foaf:organization";
	private final String PREDICATE_CLASS_YEAR = "sib:class_year";

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		String firstName = null;
		String lastName = null;
		String organization = null;
		String year = null;
		Iterator<Text> it = values.iterator();
		while (it.hasNext()) {
			// Break statement into object, predicate and subject
			String[] rdfParts = it.next().toString()
					.split(" (?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
			String predicate = rdfParts[1];
			String object = rdfParts[2];
			// if the triple is <userId foaf:firstName userFirstName>, extract
			// the first name of the user
			if (predicate.equals(PREDICATE_FIRST_NAME)) {
				firstName = object;
				// if the triple is <userId foaf:lastName userLastName>, extract
				// the last name of an user
			} else if (predicate.equals(PREDICATE_LAST_NAME)) {
				lastName = object;
				// if the triple is <userId foaf:organization organization>,
				// extract
				// the organization of an user
			} else if (predicate.equals(PREDICATE_ORGANIZATION)) {
				organization = object;
				// if the triple is <userId sib:class_year year>, extract
				// the year when the user graduated
			} else if (predicate.equals(PREDICATE_CLASS_YEAR)) {
				year = object;
			}
		}
		// if an user doesn't have information for the organization or year of
		// graduation
		// don't include it in the final result set
		if (year != null && organization != null) {
			// format found information in appropriate way
			// and write it to the output file
			String outputValue = formatOutputValue(firstName, lastName,
					organization, year);
			context.write(key, new Text(outputValue));
		}
	}

	// output pattern - "organization" "year" "full name"
	public String formatOutputValue(String firstName, String lastName,
			String organization, String year) {
		firstName = firstName.replaceAll("\"", "");
		lastName = lastName.replaceAll("\"", "");
		year = year.replace("^^xsd:date", "");
		StringBuilder sb = new StringBuilder();
		sb.append(" " + organization);
		sb.append(" " + year);
		sb.append(" \"" + firstName + " " + lastName + "\"");
		System.out.println(sb.toString());
		return sb.toString();
	}
}
