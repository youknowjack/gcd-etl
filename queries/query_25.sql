/*
Name: Snapshots
Data source: 1
Created By: Jack Humphrey
Last Updated At: 2020-08-18T05:00:46.784Z
*/
SELECT snapshot
FROM "gcd"."gcdissuesnapshot"
GROUP BY snapshot
ORDER BY snapshot DESC