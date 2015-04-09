(ns akvo-unified-log.event-log
  (:require [clojure.string :as string]
            [clojure.set :as set]
            [clojure.data :as data]
            [akvo-unified-log.json :as json]
            [clojure.pprint :refer (pprint)]
            [clojure.java.jdbc :as jdbc]
            [cheshire.core :refer (generate-string parse-string)]
            [environ.core :refer (env)]
            [clj-http.client :as http])
  (:import [org.postgresql.util PGobject]))

(def cartodb
  {:url "http://flowaglimmerofhope-hrd.cartodb.akvo.org:8080/api/v2/sql"
   :api-key (env :api-key)})

(def jonase-cartodb
  {:url "http://jonase.cartodb.com/api/v2/sql"
   :api-key "2883e2fd657dc8adaeb12e55a079dbb47de0e08d"})

(def db-spec {:subprotocol "postgresql"
              :subname "//localhost/flowaglimmerofhope-hrd"
              :user (env :database-user)
              :password (env :database-password)})


(def cartodbfy-data-points "SELECT cdb_cartodbfytable ('data_point');")

(declare queryf)

(defn setup-tables []
  (let [offset-sql "CREATE TABLE IF NOT EXISTS \"offset\" (
                       org_id TEXT UNIQUE,
                       \"offset\" BIGINT)"
        survey-sql "CREATE TABLE IF NOT EXISTS survey (
                       id BIGINT PRIMARY KEY,
                       name TEXT,
                       public BOOLEAN,
                       description TEXT);"
        form-sql "CREATE TABLE IF NOT EXISTS form (
                     id BIGINT PRIMARY KEY,
                     survey_id BIGINT,
                     name TEXT,
                     description TEXT);"
        question-sql "CREATE TABLE IF NOT EXISTS question (
                         id BIGINT PRIMARY KEY,
                         form_id BIGINT,
                         display_text TEXT,
                         identifier TEXT,
                         type TEXT);"
        data-point-sql "CREATE TABLE IF NOT EXISTS data_point (
                           id BIGINT PRIMARY KEY,
                           lat DOUBLE PRECISION,
                           lon DOUBLE PRECISION,
                           name TEXT,
                           identifier TEXT);"]
    (queryf offset-sql)
    (queryf survey-sql)
    (queryf form-sql)
    (queryf question-sql)
    (queryf data-point-sql)))

(def get-all
  "SELECT id, payload::text FROM event_log ORDER BY id ASC")

(defn get-from [offset]
  {:pre [(integer? offset)]}
  (format "SELECT id, payload::text FROM event_log WHERE id >= %s ORDER BY id ASC"
          offset))

(defn event-seq [result-set]
  (lazy-seq
   (when (.next result-set)
     (cons {:offset (.getLong result-set 1)
            :payload (parse-string (.getString result-set 2)) }
           (event-seq result-set)))))

(defmacro with-event-seq [{db-spec :db-spec events :as offset :offset} & body]
  `(with-open [conn# (jdbc/get-connection ~db-spec)]
     (.setAutoCommit conn# false)
     (with-open [stmt# (.createStatement conn#)]
       (.setFetchSize stmt# 300)
       (with-open [result-set# (.executeQuery stmt# (get-from ~offset))]
         (let [~events (event-seq result-set#)]
           ~@body)))))

(defn query [q]
  (println q)
  (http/get (:url cartodb)
            {:query-params {:q q
                            :api_key (:api-key cartodb)}}))

(defn escape-str [s]
  (string/replace s "'" "''"))

(defn queryf [q & args]
  (-> (query (apply format q args))
      :body
      parse-string
      (get "rows")))

(defn question-type->db-type [question-type]
  (condp contains? question-type
    #{"FREE_TEXT" "OPTION" "NUMBER" "PHOTO" "GEO" "SCAN" "VIDEO" "GEOSHAPE"} "text"
    #{"DATE"} "date"
    #{"CASCADE"} "text[]"))

(defn answer-type->db-type [answer-type]
  (condp contains? answer-type
    #{"VALUE" "GEO" "IMAGE" "VIDEO" "OTHER"} "text"
    #{"DATE"} "date"
    #{"CASCADE"} "text[]"))

;; Table for form instances for a specific form
;; TODO: Can we have only one table, form_instances?
(defn form-instance-table-name [form-id]
  {:pre [(integer? form-id)]}
  (str "form_instance_" form-id))

(defn question-column-name [id]
  {:pre [(integer? id)]}
  (str "question_" id))

(defmulti handle-event
  (fn [event]
    (get-in event [:payload "eventType"])))

;; Skip
(defmethod handle-event :default [_])

(defmethod handle-event "surveyGroupCreated" [{:keys [payload offset]}]
  (let [entity (get payload "entity")]
    ;; Ignore folders for now.
    (when (= (get entity "surveyGroupType")
             "SURVEY")
      (queryf "INSERT INTO survey (id, name, public, description) VALUES (%s, '%s', %s, '%s')"
                 (get entity "id")
                 (get entity "name")
                 (get entity "public")
                 (get entity "description")))))

(defmethod handle-event "surveyGroupUpdated" [{:keys [payload offset]}]
  (let [entity (get payload "entity")]
    (when (= (get entity "surveyGroupType")
             "SURVEY")
      (queryf "UPDATE survey SET name='%s', public=%s, description='%s' WHERE id=%s"
                (get entity "name")
                (get entity "public")
                (get entity "description")
                (get entity "id")))))

(defmethod handle-event "formCreated" [{:keys [payload offset]}]
  (let [entity (get payload "entity")
        form-id (get entity "id")
        table-name (form-instance-table-name form-id)]
    (queryf "CREATE TABLE IF NOT EXISTS %s (
                id BIGINT PRIMARY KEY,
                data_point_id BIGINT);"
            table-name)))

(defmethod handle-event "questionCreated" [{:keys [payload offset]}]
  (let [entity (get payload "entity")]
    (queryf "ALTER TABLE IF EXISTS %s ADD COLUMN %s %s"
            (form-instance-table-name (get entity "formId"))
            (question-column-name (get entity "id"))
            (question-type->db-type (get entity "questionType")))
    (queryf "INSERT INTO question (id, form_id, display_text, identifier, type)
               VALUES ('%s','%s','%s','%s', '%s')"
            (get entity "id")
            (get entity "formId")
            (get entity "displayText")
            (get entity "identifier" "")
            (get entity "questionType"))))

(defn get-question-type [id]
  (-> "SELECT \"type\" FROM question WHERE id=%s"
    (queryf id)
    first
    (get "type")))

(defmethod handle-event "questionUpdated" [{:keys [payload offset]}]
  ;; We don't handle a change in form-id.
  (let [new-question (get payload "entity")
        id (get new-question "id")
        question-type (get new-question "questionType")
        existing-question-type (get-question-type id)]
    (queryf "UPDATE question SET display_text='%s', identifier='%s', type='%s' WHERE id='%s'"
            (get new-question "displayText")
            (get new-question "identifier" "")
            question-type
            id)
    (when (not= question-type
                existing-question-type)
      (queryf "ALTER TABLE IF EXISTS %s ALTER COLUMN %s TYPE %s USING NULL"
              (form-instance-table-name (get new-question "formId"))
              (question-column-name id)
              (question-type->db-type question-type)))))

(defmethod handle-event "formInstanceCreated" [{:keys [payload offset]}]
  (let [form-instance (get payload "entity")]
    (queryf "INSERT INTO %s (id, data_point_id) VALUES (%s, %s)"
            (form-instance-table-name (get form-instance "formId"))
            (get form-instance "id")
            (get form-instance "dataPointId" "NULL"))))

(defmethod handle-event "formInstanceUpdated" [{:keys [payload offset]}]
  (let [form-instance (get payload "entity")]
    (queryf "UPDATE %s SET data_point_id=%s WHERE id=%s"
            (form-instance-table-name (get form-instance "formId"))
            (get form-instance "dataPointId" "NULL")
            (get form-instance "id"))))

(defmethod handle-event "dataPointCreated" [{:keys [payload offset]}]
  (let [data-point (get payload "entity")]
    (queryf "INSERT INTO data_point (id, lat, lon, name, identifier) VALUES
                 (%s, %s, %s, '%s', '%s')"
            (get data-point "id")
            (get data-point "lat")
            (get data-point "lon")
            (get data-point "name")
            (get data-point "identifier"))))

(defmethod handle-event "dataPointUpdated" [{:keys [payload offset]}]
  (let [data-point (get payload "entity")]
    (queryf "UPDATE data_point SET lat=%s, lon=%s, name='%s', identifier='%s' WHERE id=%s"
            (get data-point "lat")
            (get data-point "lon")
            (get data-point "name")
            (get data-point "identifier")
            (get data-point "id"))))

(defmethod handle-event "answerCreated" [{:keys [payload offset]}]
  (let [answer (get payload "entity")]
    (queryf "UPDATE %s SET %s=%s WHERE id=%s"
            (form-instance-table-name (get answer "formId"))
            (question-column-name (get answer "questionId"))
            (format "'%s'" (get answer "value"))
            (get answer "formInstanceId"))))

(defn delete-all-forms-and-surveys []
  (let [tables (->> (queryf "SELECT * FROM pg_tables")
                 (map #(get % "tablename"))
                 (filter #(.startsWith % "form_instance_")))]
    (queryf "DROP TABLE IF EXISTS %s;" (string/join "," tables))))

(comment

  (->> (queryf "SELECT * FROM pg_tables")
    (map #(get % "tablename"))
    (filter #(not (.startsWith % "pg_"))))


  (do
    ;; reset
    (queryf "DELETE FROM question")
    (queryf "DELETE FROM survey")
    (queryf "DELETE FROM data_point")
    (delete-all-forms-and-surveys)

    ;; start from the beginning
    (with-event-seq {:db-spec db-spec :offset 0 :as events}
      (doseq [event events]
        (try
          (handle-event event)
          (catch Exception e
            #_(println e)
            (println "Could not handle event"
                     (get event :offset)
                     (get-in event [:payload "eventType"])
                     (get-in event [:payload "entity"])))))))

)
