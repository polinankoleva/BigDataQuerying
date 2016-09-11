package ex3;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * The second mapper. It parses the information for an user and group it by
 * organization+year. For each combination of organization and year - a reducer.
 * 
 * @author Polina Koleva
 *
 */
public class UsersPerOrganizationAndYearMapper extends
		Mapper<Object, Text, Text, Text> {

	// variable where the key for the reducer will be stored
	private Text keyOut = new Text();
	private Text valueOut = new Text();

	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		// parse a line as <userId "organization" "year" "full Name">
		String[] values = value.toString().split("\t");
		String userId = values[0];
		String pattern = "(\")([a-zA-Z-,&()'. ]*\\d*)(\")";
		// Create a Pattern object
		Pattern r = Pattern.compile(pattern);
		String organization = null;
		String year = null;
		String fullName = null;
		// Now create matcher object.
		Matcher m = r.matcher(values[1]);
		if (m.find()) {
			// first match is an organization
			organization = m.group(2);
		}
		if (m.find()) {
			// second match - year
			year = m.group(2);
		}
		if (m.find()) {
			// third - user's full name
			fullName = m.group(2);
		}
		// set as a key organization+year in format (organization, year) - ready
		// to be included in the final result set
		keyOut.set("(" + organization + ", " + year + ")");
		// format the value as (userId, fullName) - ready to be included in the
		// final result set
		valueOut.set("(" + userId + ", " + fullName + ")");
		context.write(keyOut, valueOut);
	}
}
