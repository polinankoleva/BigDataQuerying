package udf;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Class that is used for testing the correct execution of
 * {@link BIRTHDAY_WITHIN_TWO_WEEKS}.
 * 
 * @author Polina Koleva
 *
 */
public class BIRTHDAY_WITHIN_TWO_WEEKS_TEST {

	static List<String> testDates;
	BIRTHDAY_WITHIN_TWO_WEEKS function = new BIRTHDAY_WITHIN_TWO_WEEKS();

	@BeforeClass
	public static void setUp() {
		testDates = new ArrayList<String>();
		// start of an year
		testDates.add("1999-01-01");
		// leap year data
		testDates.add("2000-02-29");
		// normal date
		testDates.add("1991-07-04");
		testDates.add("1993-03-03");
		testDates.add("1995-03-08");
		testDates.add("2000-03-13");
		// end of an year
		testDates.add("2008-12-31");
		// month with 30 days
		testDates.add("1989-06-30");

	}

	@Test
	public void periodBetweenTwoYearsTest() {
		String referenceDate = "2016-12-30";
		List<String> resultDates = function.getBirthdaysWithinTwoWeeks(
				testDates, referenceDate);
		assertTrue(resultDates.size() == 2);
		ArrayList<String> expectedResult = new ArrayList<String>();
		expectedResult.add("1999-01-01");
		expectedResult.add("2008-12-31");
		assertArrayEquals(expectedResult.toArray(), resultDates.toArray());
	}

	@Test
	public void periodBetweenTwoMonthsTest() {
		String referenceDate = "2016-06-30";
		List<String> resultDates = function.getBirthdaysWithinTwoWeeks(
				testDates, referenceDate);
		assertTrue(resultDates.size() == 2);
		ArrayList<String> expectedResult = new ArrayList<String>();
		expectedResult.add("1991-07-04");
		expectedResult.add("1989-06-30");
		assertArrayEquals(expectedResult.toArray(), resultDates.toArray());
	}

	@Test
	public void periodWithinOneMonthTest() {
		String referenceDate = "2016-03-01";
		List<String> resultDates = function.getBirthdaysWithinTwoWeeks(
				testDates, referenceDate);
		assertTrue(resultDates.size() == 3);
		ArrayList<String> expectedResult = new ArrayList<String>();
		expectedResult.add("1993-03-03");
		expectedResult.add("1995-03-08");
		expectedResult.add("2000-03-13");
		assertArrayEquals(expectedResult.toArray(), resultDates.toArray());
	}

	@Test
	public void leapYearReferenceYearTest() {
		String referenceDate = "2016-02-29";
		List<String> resultDates = function.getBirthdaysWithinTwoWeeks(
				testDates, referenceDate);
		assertTrue(resultDates.size() == 4);
		ArrayList<String> expectedResult = new ArrayList<String>();
		expectedResult.add("2000-02-29");
		expectedResult.add("1993-03-03");
		expectedResult.add("1995-03-08");
		expectedResult.add("2000-03-13");
		assertArrayEquals(expectedResult.toArray(), resultDates.toArray());
	}

}
