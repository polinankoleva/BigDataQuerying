REGISTER $rdfStorageFolder/RDFStorage.jar;

-- loads social graph
indata = LOAD '$input' USING RDFStorage() AS (s, p, o) ;

-- checks if a given parameter for user is of type sib:User
user = FILTER indata BY s == '$userId' AND p == 'a' AND o == 'sib:User' ;

-- gets all friends of the user $userId
userFriends = FILTER indata BY p == 'foaf:knows' AND s == user.$0 ;  

-- gets all friendships
friends = FILTER indata BY p == 'foaf:knows' ;

-- gets friends of an user's friends
friendsOfFriends = JOIN userFriends BY o, friends BY s;

-- removes those suggested friends who are already friends of the user and the user itself
friendsOfFriendsLeftOuterJoin = JOIN friendsOfFriends BY $5 LEFT OUTER, userFriends BY o;
undirectFriendOfUserWithoutUser = FILTER friendsOfFriendsLeftOuterJoin BY $5 != user.$0 ;
undirectFriendOfUser = FILTER undirectFriendOfUserWithoutUser BY ($6 IS NULL) ;

-- groups by friends of user's friends who are still not friends of an user
groupByNotAFriend = GROUP undirectFriendOfUser BY ($5) ;

-- for each "not a friend" user counts the number of user's friends who are friends of it
notFriends = FOREACH groupByNotAFriend GENERATE group, COUNT($1) AS commonFriends ;

-- orders "not a friend" users by number of common friends
orderedNotFriendsByCommonFriends = ORDER notFriends BY commonFriends DESC ;

-- gets the 10 suggested users 
tenSuggestedUsers = LIMIT orderedNotFriendsByCommonFriends 10 ;

-- stores the result into output file
STORE tenSuggestedUsers INTO '$output';
