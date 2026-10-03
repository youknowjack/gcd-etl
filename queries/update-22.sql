/*
Name: Issue Count By Snapshot
Data source: 1
Created By: Jack Humphrey
Last Updated At: 2024-02-05T18:38:54.524Z
*/
SELECT snapshot,
        count(distinct(issue_id)) as issues
FROM gcd.gcdissuesnapshot
WHERE snapshot % 10000 IN (215, 815) 
      AND variant_of_issue_id = 0 
      AND series_is_comics_publication = true
GROUP BY snapshot
ORDER BY snapshot DESC
