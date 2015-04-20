(ns akvo-unified-log.reporting
  (:require [akvo-unified-log.pg :as pg]
            [taoensso.timbre :refer (debugf infof warnf errorf fatalf error)]
            [environ.core :refer (env)]
            [clojure.string :as string]
            [clojure.walk :as walk]
            [clojure.java.jdbc :as jdbc]
            [clojure.core.async :as async :refer (<!!)]))

(comment

  (Thread/setDefaultUncaughtExceptionHandler
   (reify Thread$UncaughtExceptionHandler
     (uncaughtException [_ thread ex]
       (error ex "Unhandled exception in thread"))))

  (jdbc/query reporting-spec ["SELECT * FROM survey"])
  (jdbc/query reporting-spec ["SELECT * FROM form WHERE survey_id=?" 30134009])
  (jdbc/query reporting-spec ["SELECT * FROM question WHERE form_id=?" 25594006])
  (jdbc/query reporting-spec ["SELECT * FROM raw_data_25594006"])
  (jdbc/query reporting-spec ["SELECT name, shoe_size::integer FROM raw_data_25594006"])

  (setup-tables reporting-spec)
  (reset-tables reporting-spec)

  (def close! (restart event-log-spec
                       reporting-spec
                       (partial handle-event reporting-spec)))

  (close!))

(def event-log-spec
  {:subprotocol "postgresql"
   :subname "//localhost/flowaglimmerofhope-hrd"
   :user (env :database-user)
   :password (env :database-password)})

(def reporting-spec
  {:subprotocol "postgresql"
   :subname "//localhost:5433/flowaglimmerofhope-hrd"
   :user (env :reporting-user)
   :password (env :reporting-password)})

(defn db-spec?
  [db-spec]
  (and (map? db-spec)
       (every? #(contains? db-spec %)
               [:subprotocol :subname :user :password])))

(defn setup-tables
  [db-spec]
  {:pre [(db-spec? db-spec)]}
  (let [offset-sql "CREATE TABLE IF NOT EXISTS \"offset\" (
                      org_id TEXT PRIMARY KEY,
                      \"offset\" BIGINT)"
        survey-sql "CREATE TABLE IF NOT EXISTS survey (
                      id BIGINT PRIMARY KEY,
                      name TEXT,
                      public BOOLEAN,
                      description TEXT)"
        form-sql "CREATE TABLE IF NOT EXISTS form (
                    id BIGINT PRIMARY KEY,
                    survey_id BIGINT,
                    name TEXT,
                    description TEXT)"
        question-sql "CREATE TABLE IF NOT EXISTS question (
                        id BIGINT PRIMARY KEY,
                        form_id BIGINT,
                        display_text TEXT,
                        identifier TEXT,
                        type TEXT)"
        data-point-sql "CREATE TABLE IF NOT EXISTS data_point (
                          id BIGINT PRIMARY KEY,
                          lat DOUBLE PRECISION,
                          lon DOUBLE PRECISION,
                          name TEXT,
                          identifier TEXT)"]
    (jdbc/with-db-connection [db-conn db-spec]
      (doseq [sql [offset-sql survey-sql form-sql question-sql data-point-sql]]
        (jdbc/execute! db-conn [sql])))))

(defn reset-tables
  [db-spec]
  {:pre [(db-spec? db-spec)]}
  (jdbc/with-db-connection [db-conn db-spec]
    (let [tables ["\"offset\"" "survey" "form" "question" "data_point"]
          raw-data-tables (->> ["SELECT tablename FROM pg_tables"]
                               (jdbc/query db-conn)
                               (map :tablename)
                               (filter #(.startsWith % "raw_data_")))]
      (doseq [table tables]
        (jdbc/execute! db-conn [(str "DELETE FROM " table)]))
      (doseq [raw-data-table raw-data-tables]
        (jdbc/execute! db-conn [(str "DROP TABLE IF EXISTS " raw-data-table)])))))

(defn start
  "Start listening from offset. Returns a function that will close the connection"
  [db-spec offset event-handler]
  (let [{:keys [chan close!]} (pg/event-chan* db-spec offset)]
    (async/thread
      (loop []
        (when-let [event (async/<!! chan)]
          (try
            (let [payload (:payload event)]
              (event-handler event))
            (catch Exception e
              (errorf e "Could not handle event %s" (get-in event [:payload "eventType"]))))
          (recur))))
    close!))

(defn restart [event-log-spec target-spec event-handler]
  (reset-tables target-spec)
  (start event-log-spec 0 event-handler))

(defn wrap-update-offset [event-handler]
  (fn [db-spec event]
    (jdbc/with-db-transaction [db-conn db-spec]
      (event-handler db-conn event)
      (jdbc/update! db-conn :offset
                    {:offset (:offset event)}
                    ["org_id" (:subname db-spec)]))))

(defn raw-data-table-name [form-id]
  {:pre [(integer? form-id)]}
  (str "raw_data_" form-id))

(defmulti handle-event
  (fn [db-spec event]
    {:pre [(db-spec? db-spec)]}
    (get-in event [:payload "eventType"])))

(defmethod handle-event :default [db-spec event]
  (debugf "Skipping event %s" (get-in event [:payload "eventType"])))

(defmethod handle-event "surveyGroupCreated"
  [db-spec {:keys [payload offset]}]
  (let [entity (get payload "entity")]
    ;; Ignore folders for now.
    (when (= (get entity "surveyGroupType")
             "SURVEY")
      (jdbc/insert! db-spec :survey
                    {:id (get entity "id")
                     :name (get entity "name")
                     :public (get entity "public")
                     :description (get entity "description")}))))

(defmethod handle-event "surveyGroupUpdated"
  [db-spec {:keys [payload offset]}]
  (let [entity (get payload "entity")]
    (when (= (get entity "surveyGroupType")
             "SURVEY")
      (jdbc/update! db-spec :survey
                    {:name (get entity "name")
                     :public (get entity "public")
                     :description (get entity "description")}
                    ["id = ?" (get entity "id")]))))

(defmethod handle-event "formCreated"
  [db-spec {:keys [payload offset]}]
  (let [entity (get payload "entity")
        form-id (get entity "id")
        table-name (raw-data-table-name form-id)]
    (jdbc/with-db-connection [db-conn db-spec]
      (jdbc/execute! db-conn
                     [(format
                       "CREATE TABLE IF NOT EXISTS %s (
                          id BIGINT UNIQUE NOT NULL,
                          data_point_id BIGINT,
                          lat DOUBLE PRECISION,
                          lon DOUBLE PRECISION);"
                       table-name)])
      (jdbc/insert! db-conn :form
                    {:id form-id
                     :survey_id (get entity "surveyId")
                     :name (get entity "name" "")
                     :description (get entity "description" "")}))))

(defmethod handle-event "formUpdated"
  [db-spec {:keys [payload offset]}]
  (let [form (get payload "entity")]
    (jdbc/update! db-spec :form
                  {:survey_id (get form "surveyId")
                   :name (get form "name" "")
                   :description (get form "description" "")}
                  ["id = ?" (get form "id")])))

(defn question-type->db-type [question-type]
  (condp contains? question-type
    #{"FREE_TEXT" "OPTION" "NUMBER" "PHOTO" "GEO" "SCAN" "VIDEO" "GEOSHAPE"} "text"
    #{"DATE"} "date"
    #{"CASCADE"} "text[]"))

(defn munge-display-text [display-text]
  {:pre [(string? display-text)]}
  (-> display-text
      (.replaceAll " " "_")
      (.replaceAll "[^A-Za-z0-9_]" "")))

(defn question-column-name
  ([db-conn question-id]
   {:pre [(integer? question-id)]}
   (if-let [{:keys [display_text identifier]}
            (first (jdbc/query db-conn
                               ["SELECT display_text, identifier FROM question WHERE id=?"
                                question-id]))]
     (question-column-name question-id identifier display_text)
     (throw (ex-info "Could not find question" {:quesiton-id question-id}))))
  ([question-id identifier display-text]
   {:pre [(integer? question-id)
          (string? display-text)
          (string? identifier)]}
   (if (empty? identifier)
     (format "\"%s_%s\"" question-id (munge-display-text display-text))
     identifier)))

(defmethod handle-event "questionCreated"
  [db-spec {:keys [payload offset]}]
  (let [question (get payload "entity")]
    (jdbc/with-db-connection [db-conn db-spec]
      (jdbc/execute! db-conn
                     [(format "ALTER TABLE IF EXISTS %s ADD COLUMN %s %s"
                              (raw-data-table-name (get question "formId"))
                              (question-column-name (get question "id")
                                                    (get question "identifier" "")
                                                    (get question "displayText"))
                              (question-type->db-type (get question "questionType")))])
      (jdbc/insert! db-conn :question
                    {:id (get question "id")
                     :form_id (get question "formId")
                     :display_text (get question "displayText" "")
                     :identifier (get question "identifier" "")
                     :type (get question "questionType")}))))

(defn get-question [db-conn id]
  {:pre [(integer? id)]}
  (walk/stringify-keys
   (first (jdbc/query db-conn
                      ["SELECT display_text as \"displayText\",
                               identifier,
                               \"type\" as \"questionType\"
                        FROM question WHERE id=?"
                       id]
                      :identifiers identity))))

(defmethod handle-event "questionUpdated"
  [db-spec {:keys [payload offset] :as event}]
  (jdbc/with-db-connection [db-conn db-spec]
    (let [new-question (get payload "entity")
          id (get new-question "id")
          type (get new-question "questionType")
          display-text (get new-question "displayText")
          identifier (get new-question "identifier" "")
          existing-question (get-question db-conn id)
          existing-type (get existing-question "questionType")
          existing-display-text (get existing-question "displayText")
          existing-identifier (get existing-question "identifier" "")]
      (when-not existing-question
        (throw (ex-info "No such question" event)))
      (jdbc/update! db-conn :question
                    {:display_text display-text
                     :identifier identifier
                     :type type}
                    ["id=?" id])
      (when (or (not= display-text existing-display-text)
                (not= identifier existing-identifier))
        (jdbc/execute! db-conn
                       [(format "ALTER TABLE IF EXISTS %s RENAME COLUMN %s TO %s"
                                (raw-data-table-name (get new-question "formId"))
                                (question-column-name id existing-identifier existing-display-text)
                                (question-column-name id identifier display-text))]))
      (when (not= type existing-type)
        (jdbc/execute! db-conn
                       [(format "ALTER TABLE IF EXISTS %s ALTER COLUMN %s TYPE %s USING NULL"
                                (raw-data-table-name (get new-question "formId"))
                                (question-column-name db-conn id)
                                (question-type->db-type type))])))))

(defmethod handle-event "dataPointCreated"
  [db-spec {:keys [payload offset]}]
  (let [data-point (get payload "entity")]
    (jdbc/insert! db-spec :data_point
                  {:id (get data-point "id")
                   :lat (get data-point "lat")
                   :lon (get data-point "lon")
                   :name (get data-point "name")
                   :identifier (get data-point "identifier")})))

(defmethod handle-event "dataPointUpdated"
  [db-spec {:keys [payload offset]}]
  (let [data-point (get payload "entity")]
    (jdbc/update! db-spec :data_point
                  {:lat (get data-point "lat")
                   :lon (get data-point "lon")
                   :name (get data-point "name")
                   :identifier (get data-point "identifier")}
                  ["id=?" (get data-point "id")])))

(defn get-location [db-conn data-point-id]
  {:pre [(integer? data-point-id)]}
  (first (jdbc/query db-conn
                     ["SELECT lat, lon FROM data_point WHERE id=?" data-point-id])))


(defn contains-form-instance? [db-conn table id]
  (-> db-conn
      (jdbc/query [(format "SELECT 1 FROM %s WHERE id=?" table) id])
      empty?
      not))

;; For some reason we *always* get two identical formInstanceCreated events.
;; When this is fixed, remove the (when-not (contains-form-instance? ...))
(defmethod handle-event "formInstanceCreated"
  [db-spec {:keys [payload offset]}]
  (jdbc/with-db-connection [db-conn db-spec]
    (let [form-instance (get payload "entity")
          id (get form-instance "id")
          data-point-id (get form-instance "dataPointId")
          table (raw-data-table-name (get form-instance "formId"))
          {:keys [lat lon]} (when data-point-id (get-location db-conn data-point-id))]
      (when-not (contains-form-instance? db-conn table id)
        (jdbc/insert! db-conn table
                      {:id id
                       :data_point_id (get form-instance "dataPointId")
                       :lat lat
                       :lon lon})))))

(defmethod handle-event "formInstanceUpdated"
  [db-spec {:keys [payload offset]}]
  (jdbc/with-db-connection [db-conn db-spec]
    (let [form-instance (get payload "entity")
          data-point-id (get form-instance "dataPointId")
          {:keys [lat lon]} (when data-point-id (get-location db-conn data-point-id))]
      (jdbc/update! db-conn (raw-data-table-name (get form-instance "formId"))
                    {:data_point_id (get form-instance "dataPointId")
                     :lat lat
                     :lon lon}
                    ["id=?" (get form-instance "id")]))))

(defmethod handle-event "answerCreated"
  [db-spec {:keys [payload offset]}]
  (jdbc/with-db-connection [db-conn db-spec]
    (let [answer (get payload "entity")]
      (jdbc/update! db-conn (raw-data-table-name (get answer "formId"))
                    {(question-column-name db-conn (get answer "questionId"))
                     (get answer "value")}
                    ["id=?" (get answer "formInstanceId")]))))
