-- name: select-all
SELECT * FROM akvo_log

-- name: last-timestamp
SELECT MAX((payload->'context'->>'timestamp')::bigint)
       AS timestamp
       FROM akvo_log
       WHERE payload->>'orgId'=:org_id

-- name: insert<!
INSERT INTO akvo_log ( payload ) VALUES ( :payload )
