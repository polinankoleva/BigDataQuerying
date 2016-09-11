SELECT t.object AS user, COUNT(*) AS popularity
FROM tblsocial AS t
WHERE t.predicate='foaf:knows'
GROUP BY t.object
ORDER BY popularity DESC
LIMIT ${hiveconf:LIMIT};

