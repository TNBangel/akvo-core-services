(ns akvo-auth.core
  (:require [compojure [core :refer [GET defroutes]]
                       [handler :refer [site]]
                       [route :refer [not-found resources]]]
            [hiccup.middleware :refer [wrap-base-url]]
            [org.httpkit.server :refer [run-server]]
            [akvo-auth.html :as html]))

(defroutes ^:private routes
  (GET "/" [] (html/home))
  (resources "/")
  (not-found (html/not-found)))

(def app (wrap-base-url (site routes)))

(defn -main [& [port]]
  (run-server app {:port (if port (Integer. port) 8000)}))
