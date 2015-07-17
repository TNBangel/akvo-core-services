(ns akvo-unified-log.elastic
  (:require [clojure.core.async :as async]
            [clojurewerkz.elastisch.rest :as esr :only (connect)]
            [clojurewerkz.elastisch.rest.index :as esi :only (create)]
            [clojurewerkz.elastisch.rest.document :as esd :only (create)]
            [akvo-unified-log.json :as json]
            [akvo-unified-log.pg :as pg]))



(defn handle-event [{:keys [offset payload]}]
  (if (and (= (payload "eventType") "dataPointCreated")
        (number? (get-in payload ["entity" "lat"]))
        (number? (get-in payload ["entity" "lon"])))
    (let [lat (get-in payload ["entity" "lat"])
          lon (get-in payload ["entity" "lon"])
          doc {:location {:lat lat :lon lon}}]
      (when doc
        (prn (esd/create conn "flow" "data_point" doc))))))


(comment
  
  (def conn (esr/connect "http://127.0.0.1:9200"))
  
  (def settings {:config-folder "/home/ivan/workspace/akvo/src/akvo-flow-server-config"
                 :event-log-user "postgres"
                 :event-log-password ""
                 :event-log-server "localhost"
                 :event-log-port 5432})
  
  (def offset 0)
  (def list-of-org-ids ["akvoflow-3" "akvoflow-uat1"])
  
  (def chs (let [chs (map #(pg/event-chan* (pg/event-log-spec settings %) offset)
                       list-of-org-ids)
                 close! (fn []
                          (doseq [close-chan! (map :close! chs)]
                            (close-chan!)))
                 chan (async/merge (map :chan chs))]
             (async/thread
               (loop []
                 (when-let [event (async/<!! chan)]
                   (handle-event event)
                   (recur))))
             close!))
  
  )


(comment

  (def mapping-types {"data_point" {:properties {:location {:type "geo_point"}}}})

  (esi/create conn "flow" :mappings mapping-types)
  
  ;; importing data from csv: lat,lon
  
  (with-open [f (io/reader "/tmp/data_points.2.csv")]
    (doseq [line (csv/read-csv f)]
      (esd/create conn "flow" "data_point" {:location {:lat (Double/parseDouble (line 0)) :lon (Double/parseDouble (line 1))}})))
  
)