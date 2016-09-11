package ex1c;

/**
 * Enum that represents the custom counters used in the mapppers, reducers or
 * driver.
 * 
 * @author Polina Koleva
 *
 */
public enum CustomCounters {

	/**
	 * Enum constant used for transferring the information about the total
	 * number of users(nodes) for which the page rank is computed.
	 **/
	USERS_COUNT,
	/**
	 * Enum constant used for transferring the information when the program has
	 * to be terminated.
	 */
	TERMINATE_EXECUTION
}
