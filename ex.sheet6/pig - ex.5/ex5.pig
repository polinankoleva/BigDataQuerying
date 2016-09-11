-- registers the rdf storage jar
REGISTER $rdfStorageJar;

-- loads social graph
indata = LOAD '$input' USING RDFStorage() AS (s, p, o) ;

-- gets all posts 
posts = FILTER indata BY p == 'a' AND o == 'sib:Post';

-- get all likes
likes = FILTER indata BY p == 'sib:like';

-- joins posts and likes
joinPostLikes = JOIN posts BY s, likes BY o;

-- groups by post id and counts number of likes per each post
groupByPostId = GROUP joinPostLikes BY $0;

-- <postId, #likes per post>
likesPerPosts = FOREACH groupByPostId GENERATE $0 AS postId, (DOUBLE) COUNT(joinPostLikes) AS likesPerPost;

-- filters all replies
allReplies =  FILTER indata BY p == 'sioc:reply_of';

-- filter all comments - this set has to contain post replies as well as replies of these post replies
comments = FILTER indata BY p == 'a' AND o == 'sib:Comment';

-- gets all replies that are comments
repliesJoin = JOIN  allReplies BY s, comments BY s ;

-- <replyId sioc:reply_of postId/replyId>
replies = FOREACH repliesJoin GENERATE allReplies::s, allReplies::p, allReplies::o ; 

--joins posts and replies to receive all replies of posts
joinPostReplies = JOIN posts BY s, replies BY o;

-- all replies of posts <replyId sioc:reply_of postId>
postReplies = FOREACH joinPostReplies GENERATE $3 AS s, $4 AS p, $5 AS o;

-- joins all comments of a post with all replies by reply id. Finally we will receive
-- replies of all replies
repliesOfReplyJoin = JOIN postReplies BY s, replies BY o;

-- <replyOfReplyId sioc:reply_of postId>
postRepliesOfReply = FOREACH repliesOfReplyJoin GENERATE $3 AS s, $1 AS p, $2 AS o;

-- unions all replies of posts
allPostReplies = UNION postRepliesOfReply, postReplies;

-- groups all replies by postId
groupByPostId = GROUP allPostReplies BY $2;

-- counts how many replies a post has. Final result <postId #replies> 
allRepliesCountPerPost = FOREACH groupByPostId GENERATE $0 AS postId,
 (DOUBLE) COUNT(allPostReplies) AS countOfRepliesPerPost;
 
-- gets all dates
createdDates = FILTER indata BY p == 'dc:created';

-- joins all posts with dates to find a creation date of each post
allPostCreatedDatesJoin = JOIN posts BY s, createdDates BY s;

-- <postId, createdDate>
allPostCreatedDates = FOREACH allPostCreatedDatesJoin GENERATE posts::s AS postId, (chararray) createdDates::o AS createdDate:chararray;

-- joins all replies of posts with their creation dates
allRepliesPerPostCreatedDatesJoin = JOIN allPostReplies BY s, createdDates BY s;

-- groups by post id, finds the last reply of a post by date
groupByPostId = GROUP allRepliesPerPostCreatedDatesJoin by $2;
lastReplyPerPostBag = FOREACH groupByPostId {
 sorted = ORDER allRepliesPerPostCreatedDatesJoin BY $5 DESC;
  lim = LIMIT sorted 1;
  GENERATE lim;
};

-- <groupId, lastDate when a reply is created>
lastReplyPerPostDateWithoutCasting = FOREACH lastReplyPerPostBag GENERATE 
FLATTEN($0.allPostReplies::o)AS postId,
FLATTEN($0.createdDates::o) AS lastReplyDate;
 
-- casts the date to string
lastReplyPerPostDate = FOREACH lastReplyPerPostDateWithoutCasting GENERATE postId,
(chararray) lastReplyDate;

-- joins <postId creationDate> and <postId lastReplyCreationDate> by post id
allPostCreatedDateAndLastReplyPerPostDateJoin = JOIN allPostCreatedDates BY postId, 
lastReplyPerPostDate BY postId;

-- extracts <postId, createdDate, lastReplyCreationDate>
allPostCreatedDateAndLastReplyPerPostDate = FOREACH allPostCreatedDateAndLastReplyPerPostDateJoin 
GENERATE allPostCreatedDates::postId AS postId, REGEX_EXTRACT(allPostCreatedDates::createdDate, '"(\\d+-\\d+-\\d+T\\d+:\\d+:\\d+Z)(.*)', 1)  AS createdDate,
REGEX_EXTRACT(lastReplyPerPostDate::lastReplyDate, '"(\\d+-\\d+-\\d+T\\d+:\\d+:\\d+Z)(.*)', 1) AS lastReplyDate ;

-- calculates the difference in days between creation date of a post and its last reply date
-- <postId #days>
allPostDateDifference = FOREACH allPostCreatedDateAndLastReplyPerPostDate GENERATE $0
AS postId, DaysBetween(ToDate($2), ToDate($1)) AS difference ; 

-- joins all needed information for a post
-- <postId #likes> join <postId #replies> join <postId #differenceInDays>
postsInformation = JOIN likesPerPosts BY postId, allRepliesCountPerPost BY postId, 
allPostDateDifference BY postId;

-- generates <postId ratio(#likes/#replies) #differenceInDays>
ratiosAndDifferences = FOREACH postsInformation GENERATE likesPerPosts::postId AS postId,
likesPerPosts::likesPerPost / allRepliesCountPerPost::countOfRepliesPerPost AS ratio, 
allPostDateDifference::difference AS difference;

-- sorts by ratio and difference in days
sortedTrollPosts = ORDER ratiosAndDifferences BY ratio ASC, difference ASC;

-- gets only first 50 results
limitedSortedTrollPosts = LIMIT sortedTrollPosts 50;

-- stores the result into output file
STORE limitedSortedTrollPosts INTO '$output' ;

