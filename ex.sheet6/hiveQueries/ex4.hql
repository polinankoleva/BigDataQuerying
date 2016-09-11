-- returns friends of group's members
SELECT suggestedFriends.memberFriendId FROM
(
	-- joins the table with itself three times. First to extract the group, second - to extract members of a group
	-- and third for friends of the group's members
	-- result < groupId, member's friend Id, #friends who are already members has this member's friend>
	SELECT t1.subject AS groupId, t3.object AS memberFriendId, COUNT(*) AS friendsInGroup

	-- t1 is used for getting a specific group given by an input parameter
	FROM tblsocial2 AS t1

	-- t2 is used for getting all member of this group
	JOIN tblsocial2 AS t2

	-- joins t1 and t2 by group Id. finally receive members of the group
	ON (t1.subject = t2.subject)

	-- t3 is used for getting all friend of already selected members of a group
	JOIN tblsocial2 AS t3
	ON (t2.object = t3.subject)

	-- selects from t1 only a specific group
	WHERE t1.subject = '${hiveconf:GROUP_NAME}'
	AND t1.predicate = 'a'
	AND t1.object = 'sib:Group'

	-- selects all members
	AND t2.predicate = 'sioc:has_member'

	-- selects all friendships
	AND t3.predicate = 'foaf:knows'

	-- groups the result by group id, friend of a member
	-- for each friend of a member counts how many friend he has in this group
	GROUP BY t1.subject, t3.object
) 

-- this will stored all suggested members' friends 
AS suggestedFriends

-- gets all of suggested friends and only those which are already
-- members will have a corresponding row from the right table
-- joins by suggested friend id and member id
LEFT JOIN

(
	-- selects all members of a group. Result - <groupId groupMemberId>
	SELECT t1.subject AS groupId, 
  	t2.object AS groupMemberId FROM

	-- t1 is used for getting a specific group given by an input parameter
 	tblsocial2 AS t1

	-- t2 is used for getting all member of this group
 	JOIN tblsocial2 AS t2

	-- joins t1 and t2 by group Id. finally receive members of the group
 	ON (t1.subject = t2.subject)

	-- selects from t1 only a specific group
 	WHERE t1.subject = '${hiveconf:GROUP_NAME}'
 	AND  t1.predicate = 'a'
 	AND t1.object = 'sib:Group'

	-- selects all members of this group
 	AND t2.predicate = 'sioc:has_member'
) AS groupMembers

-- because a suggested member's friend could be already a member of a group
-- joins with all group's members to remove those from suggested friends
ON groupMembers.groupMemberId = suggestedFriends.memberFriendId

-- removes those rows where suggested friend is already member of a group
-- because we have left join if a suggested friend is not already a member
-- the row(fields) from the groupMembers table will be null
WHERE groupMembers.groupMemberId IS NULL

-- filters only those suggested friends who have already have more than 3 friends
-- which are members of a group
AND suggestedFriends.friendsInGroup >= 3;
