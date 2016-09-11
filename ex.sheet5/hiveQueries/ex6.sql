SELECT COUNT(*) AS userPostCount FROM 
tblsocial AS t1
JOIN tblsocial AS t2 
ON t1.subject = t2.object
WHERE t2.subject='${hiveconf:POST_USER}'
AND t2.predicate='sioc:creator_of'
AND t1.predicate='a'
AND t1.object='sib:Post';

