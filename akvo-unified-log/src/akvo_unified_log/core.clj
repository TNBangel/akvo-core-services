(ns akvo-unified-log.core
  (:require [clojure.pprint :refer (pprint)]
            [clojure.java.jdbc :as jdbc]
            [liberator.core :refer (resource defresource)]
            [ring.adapter.jetty :as jetty]
            [ring.middleware.params :refer (wrap-params)]
            [ring.middleware.json :refer (wrap-json-body)]
            [compojure.core :refer (defroutes ANY)]
            [yesql.core :refer (defqueries)]
            [cheshire.core :refer (generate-string)]
            [environ.core :refer (env)])
  (:import [org.postgresql.util PGobject]))

(def postgres-db {:subprotocol "postgresql"
                  :subname (env :database-url)
                  :user (env :database-user)
                  :password (env :database-password)})

(defqueries "db.sql")

(defn jsonb
  "Create a JSONB object"
  [x]
  (doto (PGobject.)
    (.setType "jsonb")
    (.setValue (generate-string x))))

(defroutes app
  (ANY "/event" [] (resource
                    :available-media-types ["application/json"]
                    :processable? (fn [ctx]
                                    (println "TODO: Validate" (-> ctx :request :body))
                                    true)
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

;; (.stop server)
;; (def server (-main))
;; (select-all postgres-db)
;; (insert<! postgres-db (str->jsonb "{}"))
