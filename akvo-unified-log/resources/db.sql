-- name: select-all
SELECT * FROM akvo_log

-- name: insert<!
INSERT INTO akvo_log ( payload ) VALUES ( :payload )
