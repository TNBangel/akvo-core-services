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

; helper functions
(defn get-double [dstr]
  "Returns nil if str cannot be cast to a double or is nil itself"
  (if (nil? dstr) 
   nil 
   (try (Double/parseDouble dstr)
    (catch NumberFormatException _))))

(defn valid-location?
  "checks if the map contains a valid lat/lon"
  [lon lat] 
  (let [latd (get-double lat)
        lond (get-double lon)]
    ;first check if we have successfully parsed the lat and lon, 
    ; then check the bounds
    (if (every? identity [latd lond])
      (and (<= -180.0 latd 180.0)
           (<= -90.0 lond 90.0 ))
      false)))

(defn geocode 
  "does a reverse geocoding of a lon lat pair"
  [lon lat]
  (select shapes (fields :name_0 :name_1 :name_2 :name_3)
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
  ; If we have a valid request, we put the lat / lon as keys in the context
  :exists? (fn [ctx] (let [{:keys [lon lat]} (get-in ctx [:request :params])] 
                       (if (valid-location? lon lat)   
                         {:lon (get-double lon) 
                          :lat (get-double lat)})))
  :handle-not-found {:status "fail" 
                     :message "Not a valid request. The lat and lon key need to be present, and -180 < lat < 180 and -90 < lon < 90"}
  :handle-ok (fn [ctx] (geocode-response (get ctx :lon) (get ctx :lat))))

(defroutes endpoints
  (ANY "/" [] ok-response)
  (ANY "/locate" [] locate-response))
  
(def handler (handler/api endpoints))  
  
(defn -main [port]
  (run-jetty #'handler {:join? false
                    :port (if port (Integer/valueOf ^String port) 8080)}))
