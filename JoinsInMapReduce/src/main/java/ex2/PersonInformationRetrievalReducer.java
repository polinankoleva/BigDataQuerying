package ex2;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * One reducer per each user. It extracts needed user's information, formats ii
 * and writes it to the output file.
 * 
 * @author Polina Koleva
 *
 */
public class PersonInformationRetrievalReducer extends
		Reducer<Text, Text, Text, Text> {

	// the searched predicates
	private final String PREDICATE_FIRST_NAME = "foaf:firstName";
	private final String PREDICATE_LAST_NAME = "foaf:lastName";
	private final String PREDICATE_BIRTHDAY = "foaf:birthday";
	private final String PREDICATE_GENDER = "foaf:gender";

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		String firstName = "";
		String lastName = "";
		String birthday = "";
		String gender = "";
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
				// if the triple is <userId foaf:birthday userBirthday>, extract
				// the birthday of an user
			} else if (predicate.equals(PREDICATE_BIRTHDAY)) {
				birthday = object;
				// if the triple is <userId foaf:gender userGender>, extract
				// the gender of an user
			} else if (predicate.equals(PREDICATE_GENDER)) {
				gender = object;
			}
		}
		// format the extracted information for an user and write it to the
		// output file
		String outputValue = formatOutputValue(firstName, lastName, birthday,
				gender);
		context.write(key, new Text(outputValue));
	}

	// format data as the output pattern - (name: Laye Sylla) (birthday:
	// 1983-02-07) (gender:
	// female)
	public String formatOutputValue(String firstName, String lastName,
			String birthday, String gender) {
		firstName = firstName.replaceAll("\"", "");
		lastName = lastName.replaceAll("\"", "");
		birthday = birthday.replaceAll("\"", "");
		birthday = birthday.replace("^^xsd:date", "");
		gender = gender.replaceAll("\"", "");
		StringBuilder sb = new StringBuilder();
		sb.append("(name: " + firstName + " " + lastName + ") ");
		sb.append("(birthday: " + birthday + ") ");
		sb.append("(gender: " + gender + ")");
		System.out.println(sb.toString());
		return sb.toString();
	}
}
