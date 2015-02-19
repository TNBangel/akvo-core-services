(ns akvo-unified-log.core
  (:require [clojure.java.io :as io]
            [clojure.pprint :refer (pprint)]
            [clojure.java.jdbc :as jdbc]
            [liberator.core :refer (resource defresource)]
            [ring.adapter.jetty :as jetty]
            [ring.middleware.params :refer (wrap-params)]
            [ring.middleware.json :refer (wrap-json-body)]
            [compojure.core :refer (defroutes ANY)]
            [yesql.core :refer (defqueries)]
            [cheshire.core :refer (generate-string)]
            [environ.core :refer (env)]
            [clj-time.core :as t])
  (:import [org.postgresql.util PGobject]
           [com.github.fge.jsonschema.main JsonSchema JsonSchemaFactory]
           [com.fasterxml.jackson.databind JsonNode]
           [com.fasterxml.jackson.databind ObjectMapper]))

(def postgres-db {:subprotocol "postgresql"
                  :subname (env :database-url)
                  :user (env :database-user)
                  :password (env :database-password)})

(defqueries "db.sql")

(def object-mapper (ObjectMapper.))

(defn json-node [s]
  (.readValue object-mapper (generate-string s) JsonNode))

(def schema-validator
  (.getJsonSchema (JsonSchemaFactory/byDefault)
                  (-> "../flow-data-schema/schema/event.json" io/file .toURI str)))

(defn valid? [s]
  (.validInstance schema-validator
                  (json-node s)))

(defn jsonb
  "Create a JSONB object"
  [x]
  (doto (PGobject.)
    (.setType "jsonb")
    (.setValue (generate-string x))))

(def instances (atom {}))

(defroutes app
  (ANY "/event-notification" []
       (resource
        :available-media-types ["application/json"]
        :allowed-methods [:post]
        :processable (fn [ctx]
                       (let [data (-> ctx :request :body)]
                         (and (contains? data "orgId")
                              (contains? data "url"))))
        :post! (fn [ctx]
                 (let [{:strs [orgId] :as event-notification} (-> ctx :request :body)]
                   (swap! instances assoc-in [orgId] (assoc event-notification
                                                            "lastNotification"
                                                            (t/now)))))))

  (ANY "/event" []
       (resource
        :available-media-types ["application/json"]
        :processable? (fn [ctx]
                        (let [event-data (-> ctx :request :body)
                              v (valid? event-data)]
                          (when-not v
                            (println "Invalid:" (-> ctx :request :body generate-string)))
                          v))
        :post! (fn [ctx]
                 (let [body (-> ctx :request :body)]
                   (insert<! postgres-db (jsonb body))))
        :allowed-methods [:post])))

(defn -main [& [port]]
  (let [port (Integer. (or port (env :port) 3030))]
    (jetty/run-jetty (-> #'app
                       wrap-params
                       wrap-json-body)
                     {:port port :join? false})))

(comment

  (.stop server)
  (def server (-main))
  (last (select-all postgres-db))
  (last-timestamp postgres-db "flowaglimmerofhope")
  (insert<! postgres-db (str->jsonb "{}"))

  )
