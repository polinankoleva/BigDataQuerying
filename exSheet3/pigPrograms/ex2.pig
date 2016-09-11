REGISTER $rdfStorageFolder/RDFStorage.jar;
-- loads social graph
indata = LOAD '$input/sibdataset200.nt' USING RDFStorage() AS (s, p, o) ;
-- filters the data by predicate 'sib:like'
filterData = FILTER indata BY p == 'sib:like' ; 
-- calculates total number of likes
likesCount = FOREACH (GROUP filterData ALL) GENERATE 'same' AS key, (DOUBLE) COUNT(filterData) AS count;
-- calculates total number of distinct users
users = FOREACH filterData GENERATE s;       
distinctUsers = DISTINCT users;                                                                                                                                                               
groupByDistinctUser = GROUP distinctUsers ALL;                                                                                                                                                          
usersCount = FOREACH groupByDistinctUser GENERATE 'same' AS key, (DOUBLE) COUNT(distinctUsers) AS count;
joinUsersAndLikesCount = JOIN likesCount BY key, usersCount BY key;
-- divide totatl likes number by total users to receive the average number of likes
likesOnAverage = FOREACH joinUsersAndLikesCount GENERATE likesCount::count / usersCount::count;
-- stores the result into output file
STORE likesOnAverage INTO '$output';
