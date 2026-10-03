/*
Name: Issue Count In 2022 Snapshots
Data source: 1
Created By: Jack Humphrey
Last Updated At: 2023-01-17T20:50:50.410Z
*/
SELECT snapshot,
        count(distinct(issue_id)) as issues
FROM gcd.gcdissuesnapshot
WHERE snapshot > 20220000 AND snapshot < 20230000
GROUP BY snapshot
ORDER BY snapshot DESC