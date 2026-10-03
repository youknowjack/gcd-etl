-- Top Writers By Decade
-- Data source: GCD DuckDB

SELECT decade, rank, writer, issues FROM (
    SELECT story.writer,
         FLOOR(publication_date/100000)*10 as decade,
         count(distinct(issue_id)) AS issues,
         row_number() over (partition by FLOOR(publication_date/100000)*10 order by count(distinct(issue_id)) desc) as rank
    FROM "gcd"."gcdissuesnapshot"
    CROSS JOIN UNNEST(story_script) AS story(writer)
    WHERE snapshot={{ snapshot }} AND
            story.writer NOT LIKE '%?%' AND story.writer != '' AND
            series_language_code = '{{ language}}' AND variant_of_issue_id = 0
    GROUP BY  story.writer, FLOOR(publication_date/100000)*10
    ORDER BY  decade DESC, issues DESC
)
WHERE rank <= {{ count }} AND decade >= {{ first_decade }} AND decade <= {{ last_decade }}
ORDER BY decade DESC, rank