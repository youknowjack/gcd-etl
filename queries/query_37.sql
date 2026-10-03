/*
Name: Unique Comic Issue Count Reaches 2 Million in 2023
Data source: 1
Created By: Jack Humphrey
Last Updated At: 2023-08-16T19:19:10.019Z
*/
SELECT snapshot,
        count(distinct(issue_id)) as issues
FROM gcd.gcdissuesnapshot
WHERE snapshot > 20230300 AND snapshot < 20230900
      AND variant_of_issue_id = 0 
      AND series_is_comics_publication = true
GROUP BY snapshot
ORDER BY snapshot DESC