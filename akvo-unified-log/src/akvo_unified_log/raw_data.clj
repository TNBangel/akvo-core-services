(ns akvo-unified-log.raw-data
  (:require [clojure.string :as string]
            [clojure.set :as set]
            [clojure.data :as data]
            [clojure.core.async :as async]
            [akvo-unified-log.json :as json]
            [akvo-unified-log.pg :as pg]
            [clojure.pprint :refer (pprint)]
            [clojure.java.jdbc :as jdbc]
            [cheshire.core :refer (generate-string parse-string)]
            [environ.core :refer (env)]
            [clj-http.client :as http])
  (:import [org.postgresql.util PGobject]))

(def cartodb
  {:url "http://flowaglimmerofhope-hrd.cartodb.akvo.org:8080/api/v2/sql"
   :api-key (env :api-key)})

(env :api-key)

(def db-spec {:subprotocol "postgresql"
              :subname "//localhost/flowaglimmerofhope-hrd"
              :user (env :database-user)
              :password (env :database-password)})

(def cartodbfy-data-points "SELECT cdb_cartodbfytable ('data_point');")

(def update-the-geom-function
  "CREATE OR REPLACE FUNCTION akvo_update_the_geom()
   RETURNS TRIGGER AS $$
   BEGIN
     NEW.the_geom := ST_SetSRID(ST_Point(NEW.lon, NEW.lat),4326);
     RETURN NEW;
   END;
   $$ language 'plpgsql';")

(declare queryf)

(defn setup-tables []
  (let [offset-sql "CREATE TABLE IF NOT EXISTS \"offset\" (
                       org_id TEXT PRIMARY KEY,
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

(comment (setup-tables)
         (queryf update-the-geom-function))


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

(defn raw-data-table-name [form-id]
  {:pre [(integer? form-id)]}
  (str "raw_data_" form-id))

(defn munge-display-text [display-text]
  {:pre [(string? display-text)]}
  (-> display-text
      (.replaceAll " " "_")
      (.replaceAll "[^A-Za-z0-9_]" "")))

(defn question-column-name
  ([question-id]
   {:pre [(integer? question-id)]}
   (if-let [{:strs [display_text identifier]}
            (-> (queryf "SELECT display_text, identifier FROM question WHERE id=%s" question-id)
                first)]
     (question-column-name question-id identifier display_text)
     (throw (ex-info "Could not find question" {:quesiton-id question-id}))))
  ([question-id identifier display-text]
   {:pre [(integer? question-id)
          (string? display-text)
          (string? identifier)]}
   (if (empty? identifier)
     (format "\"%s_%s\"" question-id (munge-display-text display-text))
     identifier)))

(defmulti handle-event
  (fn [event]
    (get-in event [:payload "eventType"])))

(defmethod handle-event :default [event]
  (println "Skipping" (get-in event [:payload "eventType"])))

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
        table-name (raw-data-table-name form-id)]
    (queryf "CREATE TABLE IF NOT EXISTS %s (
                id BIGINT UNIQUE NOT NULL,
                data_point_id BIGINT,
                lat DOUBLE PRECISION,
                lon DOUBLE PRECISION);"
            table-name)
    (queryf "INSERT INTO form (id, survey_id, name, description) VALUES (
                %s, %s, '%s', '%s')"
            form-id
            (get entity "surveyId")
            (get entity "name" "")
            (get entity "description" ""))
    (when true ;; *cartodb*
      (queryf "SELECT cdb_cartodbfytable ('%s');" table-name)
      ;; TODO Figure out why why akvo_update_the_geom trigger doesn't work
      #_(queryf "CREATE TRIGGER \"akvo_update_the_geom_trigger\"
                  BEFORE UPDATE OR INSERT ON %s FOR EACH ROW
                  EXECUTE PROCEDURE akvo_update_the_geom();"
              table-name))))

(defmethod handle-event "formUpdated" [{:keys [payload offset]}]
  (let [form (get payload "entity")]
    (queryf "UPDATE form SET survey_id=%s, name='%s', description='%s' WHERE id=%s"
            (get form "surveyId")
            (get form "name" "")
            (get form "description" "")
            (get form "id"))))

(defmethod handle-event "questionCreated" [{:keys [payload offset]}]
  (let [question (get payload "entity")]
    (queryf "ALTER TABLE IF EXISTS %s ADD COLUMN %s %s"
            (raw-data-table-name (get question "formId"))
            (question-column-name (get question "id")
                                  (get question "identifier" "")
                                  (get question "displayText"))
            (question-type->db-type (get question "questionType")))
    (queryf "INSERT INTO question (id, form_id, display_text, identifier, type)
               VALUES ('%s','%s','%s','%s', '%s')"
            (get question "id")
            (get question "formId")
            (get question "displayText")
            (get question "identifier" "")
            (get question "questionType"))))

(defn get-question [id]
  {:pre [(integer? id)]}
  (-> "SELECT display_text as \"displayText\", identifier, \"type\" as \"questionType\" FROM question WHERE id='%s'"
    (queryf id)
    first))

(defmethod handle-event "questionUpdated" [{:keys [payload offset] :as event}]
  (let [new-question (get payload "entity")
        id (get new-question "id")
        type (get new-question "questionType")
        display-text (get new-question "displayText")
        identifier (get new-question "identifier" "")
        existing-question (get-question id)
        existing-type (get existing-question "questionType")
        existing-display-text (get existing-question "displayText")
        existing-identifier (get existing-question "identifier" "")]
    (when-not existing-question
      (throw (ex-info "No such question" event)))
    (queryf "UPDATE question SET display_text='%s', identifier='%s', type='%s' WHERE id='%s'"
            display-text
            identifier
            type
            id)
    (when (or (not= display-text existing-display-text)
              (not= identifier existing-identifier))
      (queryf "ALTER TABLE IF EXISTS %s RENAME COLUMN %s TO %s"
              (raw-data-table-name (get new-question "formId"))
              (question-column-name id existing-identifier existing-display-text)
              (question-column-name id identifier display-text)))
    (when (not= type existing-type)
      (queryf "ALTER TABLE IF EXISTS %s ALTER COLUMN %s TYPE %s USING NULL"
              (raw-data-table-name (get new-question "formId"))
              (question-column-name id)
              (question-type->db-type type)))))

(defn get-location [data-point-id]
  {:pre [(integer? data-point-id)]}
  (first (queryf "SELECT lat, lon FROM data_point WHERE id=%s"
                                                 data-point-id)))

(defmethod handle-event "formInstanceCreated" [{:keys [payload offset]}]
  (let [form-instance (get payload "entity")
        data-point-id (get form-instance "dataPointId")
        {:strs [lat lon]} (when data-point-id (get-location data-point-id))]
    ;; TODO Figure out why why akvo_update_the_geom trigger doesn't work
    (queryf "INSERT INTO %s (id, data_point_id, the_geom, lat, lon) VALUES (%s, %s, %s, %s, %s)"
            (raw-data-table-name (get form-instance "formId"))
            (get form-instance "id")
            (get form-instance "dataPointId" "NULL")
            (if (and lat lon)
              (format "ST_SetSRID(ST_Point(%s, %s),4326)" lat lon)
              "NULL")
            (or lat "NULL")
            (or lon "NULL"))

    #_(queryf "INSERT INTO %s (id, data_point_id, lat, lon) VALUES (%s, %s, %s, %s)"
            (raw-data-table-name (get form-instance "formId"))
            (get form-instance "id")
            (get form-instance "dataPointId" "NULL")
            (or lat "NULL")
            (or lon "NULL"))))

(defmethod handle-event "formInstanceUpdated" [{:keys [payload offset]}]
  (let [form-instance (get payload "entity")
        data-point-id (get form-instance "dataPointId")
        {:strs [lat lon]} (when data-point-id (get-location data-point-id))]
    ;; TODO Figure out why why akvo_update_the_geom trigger doesn't work
    (queryf "UPDATE %s SET data_point_id=%s, the_geom=%s, lat=%s, lon=%s WHERE id=%s"
            (raw-data-table-name (get form-instance "formId"))
            (get form-instance "dataPointId" "NULL")
            (if (and lat lon)
              (format "ST_SetSRID(ST_Point(%s, %s),4326)" lat lon)
              "NULL")
            (or lat "NULL")
            (or lon "NULL")
            (get form-instance "id"))

    #_(queryf "UPDATE %s SET data_point_id=%s, lat=%s, lon=%s WHERE id=%s"
            (raw-data-table-name (get form-instance "formId"))
            (get form-instance "dataPointId" "NULL")
            (or lat "NULL")
            (or lon "NULL")
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
            (raw-data-table-name (get answer "formId"))
            (question-column-name (get answer "questionId"))
            (format "'%s'" (get answer "value"))
            (get answer "formInstanceId"))))

(defn delete-all-raw-data-tables []
  (when-let [tables (->> (queryf "SELECT tablename FROM pg_tables")
                 (map #(get % "tablename"))
                 (filter #(.startsWith % "raw_data_"))
                 seq)]
    (queryf "DROP TABLE IF EXISTS %s;" (string/join "," tables))))

(defn start [offset]
  (let [{:keys [chan close!] :as events} (pg/event-chan* db-spec offset)]
    (async/thread
      (loop []
          (when-let [event (async/<!! chan)]
            (try
              (let [payload (:payload event)]
                (handle-event event) )
              (catch Exception e
                (println "Could not handle event" (.getMessage e))))
            (recur)))
      close!)))

(defn restart []
  (queryf "DELETE FROM question")
  (queryf "DELETE FROM survey")
  (queryf "DELETE FROM form")
  (queryf "DELETE FROM data_point")
  (delete-all-raw-data-tables)
  (start 0))

(comment

  (def close! (restart))

  (close!)


  (->> (queryf "SELECT tablename FROM pg_tables")
    (map #(get % "tablename"))
    (filter #(not (.startsWith % "pg_"))))


  (queryf "SELECT * FROM data_points")

  (queryf "UPDATE raw_data_25594006 SET lon=26 WHERE id=33744007")
  (queryf "SELECT ST_AsGeoJSON(the_geom) AS geometry FROM raw_data_25594006")

  (queryf "select * from raw_data_25594006")

  (queryf "SELECT * FROM question")

  (queryf "SELECT CDB_UserTables()")


  (queryf "SELECT grantee, privilege_type
    FROM information_schema.role_table_grants
    WHERE table_name='raw_data_25594006'")

  (queryf "GRANT SELECT ON raw_data_25594006 TO public")

  (queryf "GRANT SELECT ON data_points TO public")

  (queryf "SELECT * FROM CDB_TableMetadata")
  (queryf "SELECT * FROM CDB_Quota")


  (queryf "SELECT * FROM data_imports")

  (Thread/setDefaultUncaughtExceptionHandler
   (reify Thread$UncaughtExceptionHandler
     (uncaughtException [this thread e]
       (.printStackTrace e))))

  )
