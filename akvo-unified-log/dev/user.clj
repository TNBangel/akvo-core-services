(ns user
  (:require [akvo-unified-log.core]
            [environ.core :refer (env)]))

(comment

  (.stop server)
  (def server (akvo-unified-log.core/-main))

  akvo-unified-log.core/instances
  akvo-unified-log.scheduler/scheduler

  (last (select-all postgres-db))
  (long (:timestamp (first (last-timestamp postgres-db))) )
  (last-fetch-date postgres-db)
  (fetch-and-insert-new-events postgres-db "s~flowaglimmerofhope-hrd")

  (let [org-id "s~flowaglimmerofhope-hrd"]
    (last-fetch-date postgres-db org-id))

  env

  )
