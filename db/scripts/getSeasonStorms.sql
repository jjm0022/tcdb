SELECT
    storms.id,
    storms.annual_id,
    storms.nhc_id,
    storms.name
FROM
    storms
WHERE
    storms.region_id <= 4
    AND
    storms.season = 2022;