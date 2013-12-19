(ns akvo-geo.core
  (:use compojure.core
        korma.core korma.db korma.sql.engine
        ring.middleware.params
        ring.middleware.multipart-params
        ring.util.response
        ring.adapter.jetty)
  (:require [liberator.core :refer [resource defresource]] 
            [cheshire.core :as json]
            [clojure.string :as str]
            [compojure [handler :as handler] [route :as route]]
            [compojure.core :refer [defroutes ANY]])
  (:gen-class))

; setup the database and PostGIS helper functions
(def pg (postgres {:db "shapes"
                   :user "markwestra"
                   :password ""
                   :host "localhost"
                   :port "5432"
                   :delimiters ""}))

(defdb korma-db pg)

(defn from-wkt [wkt]
  "Create a PostGIS geometry with geographic SRID from WKT using ST_GeomFromText."
  (sql-func "ST_GeomFromText" wkt (int 4326)))

(defn contains [first-geom second-geom]
  "An extended Korma predicate that uses the PostGIS function ST_Contains." 
  (sql-func "ST_Contains" first-geom second-geom))

(declare shapes)
(defentity shapes)

(defn geocode 
  "does a reverse geocoding of a lon lat pair"
  [lon lat]
  (select shapes (fields :name_0 :name_1 :name_2 :name_3 :name_4 :name_5)
                      (where (contains :geom (from-wkt (format "POINT(%.10f %.10f)" lat lon))))))

(defn pad-geocode-result
   "fills out the geocode data to level 5"
   ; extract the map from the vector
   [result]
   (let  [all-keys [:name_0 :name_1 :name_2 :name_3 :name_4 :name_5]]
    (reduce #(assoc %1 %2 (get result %2 "unknown")) {} all-keys)))

(defn geocode-response 
  "assembles the reverse geocoding response"
  [lon lat]
  (let [geocode-result (geocode lon lat)
        padded-result (pad-geocode-result (first geocode-result))
        license-text "Source of Administrative boundaries: The Global Administrative Unit Layers (GAUL) dataset, implemented by FAO within the CountrySTAT and Agricultural Market Information System (AMIS) projects"]
      {:status "success" 
       :license license-text 
       :data padded-result
       :lat lat
       :lon lon}))

(defresource ok-response
  :available-media-types ["application/json" "application/clojure;q=0.9"]
  :handle-ok {:status "success" :message "OK"})

(defresource locate-response
  :available-media-types ["application/json" "application/clojure;q=0.9"]
  
  :malformed? (fn [ctx]
                (let [{:keys [lat lon]} (get-in ctx [:request :params])]
                  (try
                    [false {:lat (Double/parseDouble lat) :lon (Double/parseDouble lon)}] ;; when false return a vector
                    (catch Exception _ ;; catches possible NPE and NumberFormatException
                      true))))
  :processable? (fn [ctx]
                  (boolean (and (<= -180.0 (:lat ctx) 180.0)
                                (<= -90.0 (:lon ctx) 90.0 ))))
  :handle-ok (fn [ctx]
               (geocode-response (get ctx :lon) (get ctx :lat))) ;; lat and lon available in context
  :handle-malformed {:status "fail" 
                     :message "Not a valid request. The lat and lon key need to be present"}
  :handle-unprocessable-entity {:status "fail" 
                                :message "Not a valid request. Latitude and longitude need to satistfy -180 < lat < 180 and -90 < lon < 90"})

(defroutes endpoints
  (ANY "/" [] ok-response)
  (ANY "/locate" [] locate-response))
  
(def handler (handler/api endpoints))  
  
(defn -main [port]
  (run-jetty #'handler {:join? false
                    :port (if port (Integer/valueOf ^String port) 8080)}))
