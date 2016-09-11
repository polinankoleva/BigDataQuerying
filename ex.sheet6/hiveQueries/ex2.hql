SELECT DISTINCT(posts.postId) FROM
(
	-- selects only post id of all found posts within an interval
	SELECT t.subject AS postId

	-- uses t for getting all posts
 	FROM tblsocial AS t

	-- uses t1 for getting creation dates of these posts
 	JOIN tblsocial AS t1 

	-- joins by post id
	ON t.subject = t1.subject
 	WHERE

	-- selects only posts in table t
 	t.predicate = 'a'
  	AND t.object = 'sib:Post'

	-- select only create dates within an interval in table t1
  	AND t1.predicate = 'dc:created'
  	AND regexp_extract(t1.object, '"(\\d+-\\d+-\\d+)(.*)', 1) >= '${hiveconf:START_DATE}'
  	AND regexp_extract(t1.object, '"(\\d+-\\d+-\\d+)(.*)', 1) <= '${hiveconf:END_DATE}'
) AS posts -- all posts created within a given interval of time

-- outerT is used for all hashtag or content to these already found posts
JOIN tblsocial AS outerT

-- joins by post id
ON posts.postId = outerT.subject

-- selects only those posts which hashtag or content conrain the name of event
WHERE (outerT.predicate = 'sib:hashtag' AND outerT.object LIKE '%${hiveconf:EVENT_NAME}%') OR
(outerT.predicate = 'sioc:content' AND outerT.object LIKE '%${hiveconf:EVENT_NAME}%');
