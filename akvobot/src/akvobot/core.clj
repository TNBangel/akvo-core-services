(ns akvobot.core
  (:require [ring.adapter.jetty :as jetty]
            [ring.middleware.params :refer [wrap-params]]
            [ring.middleware.json :refer [wrap-json-body]]
            [compojure.core :refer [defroutes POST GET]]
            [compojure.route :as route]
            [environ.core :refer [env]]
            [tentacles.issues :refer [create-comment]]))

(defroutes app
  (GET "/" [] "Hi there!")
  (POST "/issues" req
        (prn req))
  (route/not-found "Not found"))

(defn -main [& [port]]
  (let [port (Integer. (or port (env :port) 3001))]
    (jetty/run-jetty (-> #'app
                         wrap-params
                         wrap-json-body
                         ) {:port port :join? false})))
