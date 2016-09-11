package udf;

import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.joda.time.LocalDate;
import org.joda.time.Years;

/**
 * Pig user-defined function that computes the average age of all users. Its
 * input parameter is a list with users' birthdays. It calculates age of users
 * by their birthdays. Finally, it sums over all calculated ages and divides by
 * the number of received birthdays (which is the same as the number of users).
 * 
 * @author Polina Koleva
 *
 */
public class AVERAGE_AGE extends EvalFunc<Long> {

	public Long exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0) {
			return null;
		}
		DataBag values = (DataBag) input.get(0);
		long birthdayCount = values.size();
		long sumOfUsersAge = 0;
		Iterator<Tuple> it = values.iterator();
		while (it.hasNext()) {
			// gets a user's birthday and calculates its age
			String birthday = parseDate(it.next().get(0).toString());
			// adds user's age to already computed sum of ages
			sumOfUsersAge += getAge(birthday);
		}
		// divides total sum of users' ages by number of users(birthdays)
		return sumOfUsersAge / birthdayCount;
	}

	/**
	 * Removes unneeded characters of birthday description. For example, a
	 * common user's birthday format is "1984-12-15"^^xsd:date". The result of
	 * the method will be 1984-12-15.
	 * 
	 * @param birthdayNode
	 *            a string representation of a user's birthday
	 * @return
	 */
	public String parseDate(String birthdayNode) {
		birthdayNode = birthdayNode.replaceAll("\"", "");
		birthdayNode = birthdayNode.replace("^^xsd:date", "");
		return birthdayNode;
	}

	/**
	 * Calculates age of a user by its birthday. The birthday has to be in a
	 * format "year-month-day".
	 * 
	 * @param birthdayNode
	 *            a string representation of a user's birthday. It has to be in
	 *            a format "year-month-day"
	 * @return age of a user
	 */
	public int getAge(String birthdayNode) {
		String[] dateParts = birthdayNode.split("-");
		int year = Integer.parseInt(dateParts[0]);
		int month = Integer.parseInt(dateParts[1]);
		int day = Integer.parseInt(dateParts[2]);
		LocalDate birthday = new LocalDate(year, month, day);
		Years years = Years.yearsBetween(birthday, new LocalDate());
		return years.getYears();
	}

}
