(ns akvobot.core
  (:require [ring.adapter.jetty :as jetty]
            [ring.middleware.params :refer [wrap-params]]
            [ring.middleware.json :refer [wrap-json-body]]
            [compojure.core :refer [defroutes POST GET]]
            [compojure.route :as route]
            [environ.core :refer [env]]
            [tentacles.issues :refer [create-comment]]))

(def check-list "## Checklist\n\n* [ ] Test plan\n* [ ] Copyright header\n* [ ] Code formatting\n* [ ] Documentation")

(defn make-checklist [{:strs [action number repository]}]
  (prn action (repository "name") number)
  (if (and (= "opened" action) number (repository "name"))
    (create-comment "akvo" (repository "name") number check-list {:oauth-token (env :oauth-secret)}))
  {:status 200 :body "OK"})

(defroutes app
  (GET "/" [] "Hi there!")
  (POST "/issues" req
        (make-checklist (:body req)))
  (route/not-found "Not found"))

(defn -main [& [port]]
  (let [port (Integer/valueOf (or port (env :port) 3001))]
    (jetty/run-jetty (-> #'app
                         wrap-params
                         wrap-json-body)
                     {:port port :join? false})))
