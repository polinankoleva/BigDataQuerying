SELECT (COUNT(*)/COUNT(DISTINCT t.subject)) 
AS averegeLikesPerUser
FROM tblsocial as t
WHERE t.predicate='sib:like';
