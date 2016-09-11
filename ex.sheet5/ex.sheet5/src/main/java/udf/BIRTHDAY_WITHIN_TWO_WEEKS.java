package udf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.joda.time.LocalDate;

/**
 * Pig user-defined function which extracts all dates from a list within two
 * week from a given reference date.
 * 
 * @author Polina Koleva
 *
 */
public class BIRTHDAY_WITHIN_TWO_WEEKS extends EvalFunc<DataBag> {

	TupleFactory mTupleFactory = TupleFactory.getInstance();
	BagFactory mBagFactory = BagFactory.getInstance();

	@Override
	public DataBag exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0) {
			return null;
		}
		DataBag output = mBagFactory.newDefaultBag();
		// <userId, birthday>
		DataBag tuples = (DataBag) input.get(0);
		// start date of our period
		LocalDate startDate = getDate((String) input.get(1));
		// adds two weeks to a start date - receives end date of our period
		LocalDate endDate = startDate.plusWeeks(2);
		Iterator it = tuples.iterator();
		while (it.hasNext()) {
			Tuple tuple = (Tuple) it.next();
			// gets the user
			String user = tuple.get(0).toString();
			// gets the user's birthday
			String birthday = parseDate(tuple.get(1).toString());
			// gets the user's birthday. NOTE: because the calculation if a
			// birthday is
			// between two dates takes into account only month and day of a
			// date,
			// the return birthday will have format "month-day"
			String targetDate = getBirthday(birthday);
			// calculates if the birthday is in the interval
			if (isBeetween(startDate.toString(), endDate.toString(), targetDate)) {
				output.add(mTupleFactory.newTuple(user));
			}
		}
		return output;
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
	 * Converts a given date in a string representation into {@link LocalDate}
	 * object. The format of input parameter has to be "year-month-day".
	 * 
	 * @param date
	 *            a string representation of date. Its format has to be
	 *            "year-month-day"
	 * @return {@link LocalDate}
	 */
	public LocalDate getDate(String date) {
		String[] dateParts = date.split("-");
		int year = Integer.parseInt(dateParts[0]);
		int month = Integer.parseInt(dateParts[1]);
		int day = Integer.parseInt(dateParts[2]);
		return new LocalDate(year, month, day);
	}

	/**
	 * From a date in a format "year-month-day" extracts the month and the day.
	 * For example, if the given date is "2015-01-13", the returned result is
	 * "01-13".
	 * 
	 * @param date
	 * @return
	 */
	public String getBirthday(String date) {
		String[] dateParts = date.split("-");
		int month = Integer.parseInt(dateParts[1]);
		int day = Integer.parseInt(dateParts[2]);
		return month + "-" + day;
	}

	/**
	 * Calculates if a targetDate is between startDate and endDate without
	 * taking into account their year. So if a target date is "09-12", and we
	 * have interval [2016-09-11, 2016-09-25], the method returns true. On the
	 * other hand, if we have the same interval, but a target date is "08-13",
	 * the result is false.
	 * 
	 * @param startDate "year-month-day"
	 * @param endDate "year-month-day"
	 * @param targetDate "month-day"
	 * @return
	 */
	public boolean isBeetween(String startDate, String endDate,
			String targetDate) {

		// parses the initial date of a period
		// extracts only its month and day
		String[] startDateParts = startDate.split("-");
		int startMonth = Integer.parseInt(startDateParts[1]);
		int startDay = Integer.parseInt(startDateParts[2]);

		// parses the end date of a period
		// extracts only its month and day
		String[] endDateParts = endDate.split("-");
		int endMonth = Integer.parseInt(endDateParts[1]);
		int endDay = Integer.parseInt(endDateParts[2]);

		// parses the target date that will be checked
		// extracts its month and day
		String[] targetDateParts = targetDate.split("-");
		int targetMonth = Integer.parseInt(targetDateParts[0]);
		int targetDay = Integer.parseInt(targetDateParts[1]);

		// if the period is within one month and the target date
		// is in the same month
		if (startMonth == endMonth && startMonth == targetMonth) {
			return (targetDay >= startDay && targetDay <= endDay);
		// if the period is within two months
		} else {
			// if the target date is on the same month
			// as the start date month
			if (targetMonth == startMonth) {
				return (targetDay >= startDay && targetDay >= endDay);
			// if the target date is on the same month
			// as the end date month
			} else if (targetMonth == endMonth) {
				return (targetDay <= startDay && targetDay <= endDay);
			}
		}
		return false;
	}

	// Method which does exactly the same as "exec" method above
	// but for different DS. This method is used only for testing.
	public List<String> getBirthdaysWithinTwoWeeks(List<String> birthdays,
			String referenceDate) {
		List<String> resultBirthdays = new ArrayList<String>();
		LocalDate startDate = getDate(referenceDate);
		// adds two weeks to a start date - receives end date of our period
		LocalDate endDate = startDate.plusWeeks(2);
		for (int i = 0; i < birthdays.size(); i++) {
			// gets the user's birthday
			String birthday = parseDate(birthdays.get(i));
			// gets the user's birthday. NOTE: because the calculation if a
			// birthday is
			// between two dates takes into account only month and day of a
			// date,
			// the return birthday will have format "month-day"
			String targetDate = getBirthday(birthday);
			// calculates if the birthday is in the interval
			if (isBeetween(startDate.toString(), endDate.toString(), targetDate)) {
				resultBirthdays.add(birthday);
			}
		}
		return resultBirthdays;
	}
}
