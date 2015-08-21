(ns akvo-unified-log.elastic
  (:require [clojure.core.async :as async]
            [clojurewerkz.elastisch.rest :as esr]
            [clojurewerkz.elastisch.rest.index :as esi]
            [clojurewerkz.elastisch.rest.document :as esd]
            [akvo-unified-log.json :as json]
            [akvo-unified-log.pg :as pg]
            [clj-time.format :as f]
            [clojure.java.io :as io])
  (:import geocode.ReverseGeoCode))



(comment

  (def geocoder (ReverseGeoCode. (io/input-stream "/home/akvodash/allCountries.txt") true))

  (def conn (esr/connect "http://127.0.0.1:9200"))

  (def fmt (f/formatters :date-time))

  (def mapping-types {"data_point" {:properties {:location {:type "geo_point"}
                                                 :country {:type "string"
                                                           :index "not_analyzed"
                                                           :null_value "N/A"}
                                                 :city {:type "string"
                                                        :index "not_analyzed"
                                                        :null_value "N/A"}
                                                 :timestamp {:type "date"}}}})

  (esi/create conn "flow" :mappings mapping-types)

  (esd/create conn "flow" "data_point" {:location {:lat 42.812183 :lon -1.649408}
                                        :country "ES"
                                        :city "Pamplona"
                                        :timestamp (.print fmt 0)})

  (defn handle-event [{:keys [offset payload]}]
    (if (and (= (payload "eventType") "dataPointCreated")
          (number? (get-in payload ["entity" "lat"]))
          (number? (get-in payload ["entity" "lon"])))
      (let [lat (get-in payload ["entity" "lat"])
            lon (get-in payload ["entity" "lon"])
            ts (.print fmt (.longValue (get-in payload ["context" "timestamp"])))
            geo-name (.nearestPlace geocoder lat lon)
            doc {:location {:lat lat :lon lon}
                 :country (.country geo-name)
                 :city (.name geo-name)
                 :timestamp ts}]
        (when doc
          (prn (esd/create conn "flow" "data_point" doc))))))



  (def settings {:config-folder "/home/akvodash/akvo-flow-server-config"
                 :event-log-user "postgres"
                 :event-log-password ""
                 :event-log-server "localhost"
                 :event-log-port 5432})

  (def offset 0)

  (def list-of-org-ids ["akvoflow-35"])

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