REGISTER $rdfStorageFolder/RDFStorage.jar;
-- loads social graph
indata = LOAD '$input/sibdataset200.nt' USING RDFStorage() AS (s, p, o) ;
-- filters the data by predicate 'foaf:knows'
filterData = FILTER indata BY p == 'foaf:knows' ; 
-- removes the predicate because we don't need it anymore
usersInEdges = FOREACH filterData GENERATE s, o ; 
-- makes a group by an object because we want to count ingoing edges we have to group by object
groupByObject = GROUP usersInEdges BY o ;
-- counts for each object, the number of its ingoing edges. It is assumed as popularity.
usersCount = FOREACH groupByObject GENERATE $0 AS user, COUNT(usersInEdges) AS popularity;
-- orders the received result by popularity(#ingoing edges) DESC
orderedUsersCount = ORDER usersCount BY popularity DESC ;
-- selects only the first k users
limitedUsersCount = LIMIT orderedUsersCount $k ;
-- removes the popularity - it is not needed for the final result set
finalUsers = FOREACH limitedUsersCount GENERATE user ;
-- stores the result into output file
STORE finalUsers INTO '$output';