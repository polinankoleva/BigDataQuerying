REGISTER $rdfStorageFolder/RDFStorage.jar;
-- loads social graph
indata = LOAD '$input/sibdataset200.nt' USING RDFStorage() AS (s, p, o) ;
-- filters the data by predicate 'sib:Post' - retrieve all posts
allPosts = FILTER indata BY o == 'sib:Post' AND p == 'a' ; 
-- removes the predicate and the object - only post identifiers are needed
allPostsIds = FOREACH allPosts GENERATE s ; 
-- filters the data by predicate 'sioc:creator_of' - retrieve all users that create something
creators = FILTER indata BY p == 'sioc:creator_of' ; 
-- removes the predicate - it is not needed anymore
creatorsWithoutPredicate = FOREACH creators GENERATE s, o ; 
-- joins the two data set by post's identifier
joinResult = JOIN creatorsWithoutPredicate BY o, allPostsIds BY s ;
-- groups the joined result by user and for each user computes the number of created posts
groupByObject = GROUP joinResult BY $0;
postsPerUser = FOREACH groupByObject GENERATE $0 AS user, COUNT(joinResult) AS postsCreated;
-- stores the result into the output file 
STORE postsPerUser INTO '$output';