-- registers the rdf storage jar
REGISTER $rdfStorageJar;

-- registers the user-defined functions jar
REGISTER $udfJar;

-- loads social graph
indata = LOAD '$input' USING RDFStorage() AS (s, p, o) ;

-- gets all birthdays
birthdays = FILTER indata BY p == 'foaf:birthday' ;

-- checks if the given parameter for a user is of type sib:User
user = FILTER indata BY s == '$userId' AND p == 'a' AND o == 'sib:User' ;

-- gets all friends of the user $userId
userFriends = FILTER indata BY p == 'foaf:knows' AND s == user.$0 ; 

-- gets all accounts
accounts = FILTER indata BY p == 'sioc:account_of' ;

-- gets accounts of user's friends, removes their predicate
userFriendsAccountsJoin = JOIN userFriends BY o, accounts BY s;
userFriendsAccounts = FOREACH userFriendsAccountsJoin GENERATE accounts::s, accounts::o;

-- joins user's friends accounts and birthdays
-- receives birthdays of user's friends
userFriendsBirthdaysJoin = JOIN userFriendsAccounts BY o, birthdays BY s;
-- finally <userId birthday> structure
userFriendsBirthdays = FOREACH userFriendsBirthdaysJoin GENERATE $0 , $4;

-- calculates only those user's friends who have birthday within two weeks from
-- a given date $referenceDate. It has to have format "year-month-day"
usersWithBirthdayWithinTwoWeeks = FOREACH (group userFriendsBirthdays all) GENERATE 
udf.BIRTHDAY_WITHIN_TWO_WEEKS(userFriendsBirthdays, '$referenceDate');

-- stores the result into output file
STORE usersWithBirthdayWithinTwoWeeks INTO '$output' ;