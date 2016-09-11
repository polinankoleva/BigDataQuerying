-- returns count of inactive users
SELECT COUNT(users.userId) FROM
(
	-- selects all users
	SELECT DISTINCT(users.subject) AS userId FROM
	tblsocial as users
  	WHERE users.predicate = 'a'
 	AND users.object = 'sib:User'
)  AS users
LEFT JOIN
(
 	SELECT DISTINCT(t1.subject) AS userId

	-- uses t1 for selecting users
 	FROM tblsocial AS t1
	
	-- uses t2 for selecting creators
 	JOIN tblsocial AS t2 
	
	-- joins users and creators
	ON (t1.subject = t2.subject)

	-- uses t3 for selecting posts
  	JOIN tblsocial AS t3 

	-- joins on created by creators things and posts
	ON (t2.object = t3.subject)

	-- uses t4 for extracting the created dates
  	JOIN tblsocial AS t4 
	
	-- joins posts and created dates
	ON (t3.subject = t4.subject)
	
	-- selects users
  	WHERE t1.predicate = 'a'
  	AND t1.object = 'sib:User'

	-- selects creators
  	AND t2.predicate = 'sioc:creator_of'

	-- selects posts
  	AND t3.predicate = 'a'
  	AND t3.object = 'sib:Post'
	
	-- selects created dates within an interval
  	AND t4.predicate = 'dc:created'
  	AND regexp_extract(t4.object, '"(\\d+-\\d+-\\d+)(.*)', 1) >= '${hiveconf:START_DATE}'
  	AND regexp_extract(t4.object, '"(\\d+-\\d+-\\d+)(.*)', 1) <= '${hiveconf:END_DATE}'
) AS user_posts

-- joins users and created posts within the interval.
ON users.userId = user_posts.userId

-- because it is a left join - if an user doesn't not have a corresponding 
-- row from user_posts table - this user is inactive (doesnot have posts within the interval)
WHERE user_posts.userId IS NULL;
