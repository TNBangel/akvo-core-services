(ns akvo-unified-log.core
  (:require [clojure.tools.cli :as cli]
            [clojure.edn :as edn]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.pprint :as pp]
            [compojure.core :refer (defroutes GET routes)]
            [ring.adapter.jetty :as jetty]
            [akvo-unified-log.consumer :as consumer]
            [akvo-unified-log.raw-data :as cartodb]
            [akvo.commons.config :as config]
            [taoensso.timbre :as timbre]))

(def cli-options [[nil "--consumer CONSUMER" "Consumer to run. One of reporting/cartodb"
                   :validate [(partial #{:reporting :cartodb})
                              "Must be either 'reporting' or 'cartodb'"]
                   :parse-fn keyword]

                  [nil "--settings SETTINGS_FILE" "Path to settings file"]

                  [nil "--log-level LOGLEVEL" "Log level. One of trace/debug/info/warn/error"
                   :validate [(partial #{:trace :debug :info :warn :error})
                              "Must be one of trace/debug/info/warn/error"]
                   :default :warn
                   :parse-fn keyword]

                  [nil "--restart" "Restart all consumers from offset 0"
                   :id :restart?]

                  [nil "--port PORT" "Webserver port"
                   :default 3030
                   :parse-fn #(Long/parseLong %) ]

                  ["-h" "--help" "Show cli help summary"]])



(defn help [{:keys [summary]}]
  (println "-----------------------------------")
  (println "The following options are available")
  (println summary))

(defn validate-cli-opts [{:keys [options errors org-ids]
                          :as cli}]
  (letfn [(abort [msg]
            (println msg)
            (help cli)
            (System/exit 1))]
    (cond
      (not (empty? errors))
      (abort (apply str errors))

      (empty? org-ids)
      (abort "No org-ids specified")

      (nil? (:settings options))
      (abort "No settings file specified")

      (nil? (:consumer options))
      (abort "No consumer specified")

      :else cli)))

(defn app [opts]
  (routes
   (GET "/" []
        (format "<pre>%s</pre>"
                (str/escape (with-out-str (pp/pprint opts))
                            {\< "&lt;" \> "&gt;"})))))


(defn -main [& args]
  (let [cli-opts (cli/parse-opts args cli-options)]
    (if (get-in cli-opts [:options :help])
      (help cli-opts)
      (let [opts (validate-cli-opts (set/rename-keys cli-opts {:arguments :org-ids}))
            ;; flatten
            opts (merge (dissoc opts :options :summary :errors)
                        (:options opts))]
        (timbre/set-level! (:log-level opts))
        (config/set-settings! (:settings opts))
        (config/set-config! (@config/settings :config-folder))
        (condp :consumer opts
          :cartodb
          (doseq [org-id (:org-ids opts)]
            (consumer/start (cartodb/consumer opts org-id)))

          :reporting
          (throw (IllegalArgumentException. "TODO: Reporting cli")))
        (let [port (Integer. (:port opts))]
          (jetty/run-jetty (app opts)
                           {:port port
                            :join? false}))))))
