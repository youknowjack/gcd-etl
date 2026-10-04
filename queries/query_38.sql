/*
Name: Top Writers By Decade
Data source: 1
Created By: Jack Humphrey
Last Updated At: 2023-12-16T16:30:25.250Z
*/
SELECT decade, rank, writer, issues FROM (
    SELECT story.writer,
         FLOOR(publication_date/100000)*10 as decade,
         count(distinct(issue_id)) AS issues,
         row_number() over (partition by FLOOR(publication_date/100000)*10 order by count(distinct(issue_id)) desc) as rank
    FROM (
        SELECT issue_id, publication_date, story_script
        FROM gcd.gcdissuesnapshot
        WHERE snapshot={{ snapshot }} AND series_language_code = '{{ language}}' AND variant_of_issue_id = 0
    )
    CROSS JOIN UNNEST(story_script) AS story(writer)
    WHERE story.writer NOT LIKE '%?%' AND story.writer != ''
    GROUP BY story.writer, FLOOR(publication_date/100000)*10
    ORDER BY decade DESC, issues DESC
)
WHERE rank <= {{ count }} AND decade >= {{ first_decade }} AND decade <= {{ last_decade }}
ORDER BY decade DESC, rank
