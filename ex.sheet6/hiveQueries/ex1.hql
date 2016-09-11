-- returns id of a post, number of hashtags for this post
SELECT posts.postId, COUNT(*) as hashTagsCount FROM
(
	SELECT t.subject AS postId

	-- t will be used for filtering all posts
 	 FROM tblsocial AS t

	-- t1 will be used for filtering all created dates
  	JOIN tblsocial AS t1 

	-- joins the table with itself by subject - postId
 	 ON t.subject = t1.subject
  	WHERE t.predicate = 'a'
  	AND t.object = 'sib:Post'
  	AND t1.predicate = 'dc:created'

	-- gets only those created dates within an interval
 	 AND regexp_extract(t1.object, '"(\\d+-\\d+-\\d+)(.*)', 1) >= '${hiveconf:START_DATE}'
  	AND regexp_extract(t1.object, '"(\\d+-\\d+-\\d+)(.*)', 1) <= '${hiveconf:END_DATE}'
) AS posts -- finally we receive all id of posts created within an interval

-- joins all found posts with all hashtags by post Id. The result will be all hashtags for the found posts.
JOIN tblsocial AS outerT
ON posts.postId = outerT.subject
WHERE outerT.predicate = 'sib:hashtag'

-- groups the found hashtags by post Id, so for each found post we can compute the number of hashtags for it.
GROUP BY posts.postId

-- orders the results(posts) by the number of hashtags DESC
ORDER BY hashTagsCount DESC

-- gets the first 10 hottest events
LIMIT 10;
