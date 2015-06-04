(ns akvo-unified-log.raw-data
  (:require [clojure.string :as string]
            [clojure.set :as set]
            [clojure.edn :as edn]
            [clojure.data :as data]
            [clojure.core.async :as async]
            [akvo-unified-log.json :as json]
            [akvo-unified-log.pg :as pg]
            [akvo-unified-log.entity-store :as entity-store]
            [akvo.commons.config :as config]
            [clojure.pprint :refer (pprint)]
            [clojure.java.jdbc :as jdbc]
            [cheshire.core :refer (generate-string parse-string)]
            [environ.core :refer (env)]
            [clj-http.client :as http])
  (:import [org.postgresql.util PGobject]))

(defn cartodb-spec [config org-id]
  (let [api-key (get-in config [org-id :cartodb-api-key])
        sql-api (get-in config [org-id :cartodb-sql-api])]
    (assert api-key "Cartodb api key is missing")
    (assert sql-api "Cartodb sql api url is missing")
    {:url sql-api
     :api-key api-key
     :org-id org-id}))

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

(defn setup-tables [cdb-spec]
  (let [offset-sql "CREATE TABLE IF NOT EXISTS event_offset (
                       org_id TEXT PRIMARY KEY,
                       event_offset BIGINT)"
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
    (queryf cdb-spec offset-sql)
    (queryf cdb-spec survey-sql)
    (queryf cdb-spec form-sql)
    (queryf cdb-spec question-sql)
    (queryf cdb-spec data-point-sql)))

(defn query [cdb-spec q]
  (println q)
  (http/get (:url cdb-spec)
            {:query-params {:q q
                            :api_key (:api-key cdb-spec)}}))

(defn escape-str [s]
  (string/replace s "'" "''"))

(defn queryf [cdb-spec q & args]
  (-> (query cdb-spec (apply format q args))
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
  ([cdb-spec question-id]
   {:pre [(integer? question-id)]}
   (if-let [{:strs [display_text identifier]}
            (-> (queryf cdb-spec
                        "SELECT display_text, identifier FROM question WHERE id=%s"
                        question-id)
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
  (fn [cdb-spec event]
    (get-in event [:payload "eventType"])))

(defmethod handle-event :default [cdb-spec event]
  (println "Skipping" (get-in event [:payload "eventType"])))

(defmethod handle-event "surveyGroupCreated"
  [cdb-spec {:keys [payload offset]}]
  (let [entity (get payload "entity")]
    ;; Ignore folders for now.
    (when (= (get entity "surveyGroupType")
             "SURVEY")
      (queryf cdb-spec
              "INSERT INTO survey (id, name, public, description) VALUES (%s, '%s', %s, '%s')"
              (get entity "id")
              (get entity "name")
              (get entity "public")
              (get entity "description")))))

(defmethod handle-event "surveyGroupUpdated"
  [cdb-spec {:keys [payload offset]}]
  (let [entity (get payload "entity")]
    (when (= (get entity "surveyGroupType")
             "SURVEY")
      (queryf cdb-spec
              "UPDATE survey SET name='%s', public=%s, description='%s' WHERE id=%s"
              (get entity "name")
              (get entity "public")
              (get entity "description")
              (get entity "id")))))

(defmethod handle-event "formCreated"
  [cdb-spec {:keys [payload offset]}]
  (let [entity (get payload "entity")
        form-id (get entity "id")
        table-name (raw-data-table-name form-id)]
    (queryf cdb-spec
            "CREATE TABLE IF NOT EXISTS %s (
                id BIGINT UNIQUE NOT NULL,
                data_point_id BIGINT,
                lat DOUBLE PRECISION,
                lon DOUBLE PRECISION);"
            table-name)
    (queryf cdb-spec
            "INSERT INTO form (id, survey_id, name, description) VALUES (
                %s, %s, '%s', '%s')"
            form-id
            (get entity "surveyId")
            (get entity "name" "")
            (get entity "description" ""))
    (when true ;; *cartodb*
      (queryf cdb-spec "SELECT cdb_cartodbfytable ('%s');" table-name)
      ;; TODO Figure out why why akvo_update_the_geom trigger doesn't work
      #_(queryf cdb-spec
                "CREATE TRIGGER \"akvo_update_the_geom_trigger\"
                  BEFORE UPDATE OR INSERT ON %s FOR EACH ROW
                  EXECUTE PROCEDURE akvo_update_the_geom();"
                table-name))))

(defmethod handle-event "formUpdated"
  [cdb-spec {:keys [payload offset]}]
  (let [form (get payload "entity")]
    (queryf cdb-spec
            "UPDATE form SET survey_id=%s, name='%s', description='%s' WHERE id=%s"
            (get form "surveyId")
            (get form "name" "")
            (get form "description" "")
            (get form "id"))))

(defmethod handle-event "questionCreated"
  [cdb-spec {:keys [payload offset]}]
  (let [question (get payload "entity")]
    (queryf cdb-spec
            "ALTER TABLE IF EXISTS %s ADD COLUMN %s %s"
            (raw-data-table-name (get question "formId"))
            (question-column-name (get question "id")
                                  (get question "identifier" "")
                                  (get question "displayText"))
            (question-type->db-type (get question "questionType")))
    (queryf cdb-spec
            "INSERT INTO question (id, form_id, display_text, identifier, type)
               VALUES ('%s','%s','%s','%s', '%s')"
            (get question "id")
            (get question "formId")
            (get question "displayText")
            (get question "identifier" "")
            (get question "questionType"))))

(defn get-question [cdb-spec id]
  {:pre [(integer? id)]}
  (first
   (queryf cdb-spec
           "SELECT display_text as \"displayText\",
                   identifier,
                   \"type\" as \"questionType\"
            FROM question WHERE id='%s'"
           id)))

(defmethod handle-event "questionUpdated"
  [cdb-spec {:keys [payload offset] :as event}]
  (let [new-question (get payload "entity")
        id (get new-question "id")
        type (get new-question "questionType")
        display-text (get new-question "displayText")
        identifier (get new-question "identifier" "")
        existing-question (get-question cdb-spec id)
        existing-type (get existing-question "questionType")
        existing-display-text (get existing-question "displayText")
        existing-identifier (get existing-question "identifier" "")]
    (when-not existing-question
      (throw (ex-info "No such question" event)))
    (queryf cdb-spec
            "UPDATE question SET display_text='%s', identifier='%s', type='%s' WHERE id='%s'"
            display-text
            identifier
            type
            id)
    (when (or (not= display-text existing-display-text)
              (not= identifier existing-identifier))
      (queryf cdb-spec
              "ALTER TABLE IF EXISTS %s RENAME COLUMN %s TO %s"
              (raw-data-table-name (get new-question "formId"))
              (question-column-name id existing-identifier existing-display-text)
              (question-column-name id identifier display-text)))
    (when (not= type existing-type)
      (queryf cdb-spec
              "ALTER TABLE IF EXISTS %s ALTER COLUMN %s TYPE %s USING NULL"
              (raw-data-table-name (get new-question "formId"))
              (question-column-name cdb-spec id)
              (question-type->db-type type)))))

(defn get-location [cdb-spec data-point-id]
  {:pre [(integer? data-point-id)]}
  (first (queryf cdb-spec
                 "SELECT lat, lon FROM data_point WHERE id=%s"
                 data-point-id)))

(defmethod handle-event "formInstanceCreated"
  [cdb-spec {:keys [payload offset]}]
  (let [form-instance (get payload "entity")
        data-point-id (get form-instance "dataPointId")
        {:strs [lat lon]} (when data-point-id (get-location cdb-spec data-point-id))]
    ;; TODO Figure out why why akvo_update_the_geom trigger doesn't work
    (queryf cdb-spec
            "INSERT INTO %s (id, data_point_id, the_geom, lat, lon) VALUES (%s, %s, %s, %s, %s)"
            (raw-data-table-name (get form-instance "formId"))
            (get form-instance "id")
            (get form-instance "dataPointId" "NULL")
            (if (and lat lon)
              (format "ST_SetSRID(ST_Point(%s, %s),4326)" lat lon)
              "NULL")
            (or lat "NULL")
            (or lon "NULL"))
    #_(queryf cdb-spec
              "INSERT INTO %s (id, data_point_id, lat, lon) VALUES (%s, %s, %s, %s)"
              (raw-data-table-name (get form-instance "formId"))
              (get form-instance "id")
              (get form-instance "dataPointId" "NULL")
              (or lat "NULL")
              (or lon "NULL"))))

(defmethod handle-event "formInstanceUpdated"
  [cdb-spec {:keys [payload offset]}]
  (let [form-instance (get payload "entity")
        data-point-id (get form-instance "dataPointId")
        {:strs [lat lon]} (when data-point-id (get-location cdb-spec data-point-id))]
    ;; TODO Figure out why why akvo_update_the_geom trigger doesn't work
    (queryf cdb-spec
            "UPDATE %s SET data_point_id=%s, the_geom=%s, lat=%s, lon=%s WHERE id=%s"
            (raw-data-table-name (get form-instance "formId"))
            (get form-instance "dataPointId" "NULL")
            (if (and lat lon)
              (format "ST_SetSRID(ST_Point(%s, %s),4326)" lat lon)
              "NULL")
            (or lat "NULL")
            (or lon "NULL")
            (get form-instance "id"))
    #_(queryf cdb-spec
              "UPDATE %s SET data_point_id=%s, lat=%s, lon=%s WHERE id=%s"
              (raw-data-table-name (get form-instance "formId"))
              (get form-instance "dataPointId" "NULL")
              (or lat "NULL")
              (or lon "NULL")
              (get form-instance "id"))))

(defmethod handle-event "dataPointCreated"
  [cdb-spec {:keys [payload offset]}]
  (let [data-point (get payload "entity")]
    (queryf cdb-spec
            "INSERT INTO data_point (id, lat, lon, name, identifier) VALUES
                 (%s, %s, %s, '%s', '%s')"
            (get data-point "id")
            (get data-point "lat")
            (get data-point "lon")
            (get data-point "name")
            (get data-point "identifier"))))

(defmethod handle-event "dataPointUpdated"
  [cdb-spec {:keys [payload offset]}]
  (let [data-point (get payload "entity")]
    (queryf cdb-spec
            "UPDATE data_point SET lat=%s, lon=%s, name='%s', identifier='%s' WHERE id=%s"
            (get data-point "lat")
            (get data-point "lon")
            (get data-point "name")
            (get data-point "identifier")
            (get data-point "id"))))

(defmethod handle-event "answerCreated"
  [cdb-spec {:keys [payload offset]}]
  (let [answer (get payload "entity")]
    (queryf cdb-spec
            "UPDATE %s SET %s=%s WHERE id=%s"
            (raw-data-table-name (get answer "formId"))
            (question-column-name cdb-spec (get answer "questionId"))
            (format "'%s'" (get answer "value"))
            (get answer "formInstanceId"))))

(defn delete-all-raw-data-tables [cdb-spec]
  (when-let [tables (->> (queryf cdb-spec "SELECT tablename FROM pg_tables")
                         (map #(get % "tablename"))
                         (filter #(.startsWith % "raw_data_"))
                         seq)]
    (queryf cdb-spec "DROP TABLE IF EXISTS %s;" (string/join "," tables))))

(defn get-offset [cdb-spec org-id]
  (let [offset (-> (queryf cdb-spec
                       "SELECT event_offset FROM event_offset WHERE org_id='%s'"
                       org-id)
                   first
                   (get "event_offset"))]
    (if (nil? offset)
      (do (queryf cdb-spec
                  "INSERT INTO event_offset (org_id, event_offset) VALUES ('%s', 0)"
                  org-id)
          0)
      offset)))

(defn wrap-update-offset [config org-id event-handler]
  (let [cdb-spec (cartodb-spec config org-id)]
    (fn [event]
      (try
        (event-handler cdb-spec event)
        (queryf cdb-spec
                "UPDATE event_offset SET event_offset=%s WHERE org_id='%s'"
                (:offset event)
                (:org-id cdb-spec))
        (catch Exception e
          (println "Could not handle event" (.getMessage e)))))))

(defn start [config org-id event-handler]
  (let [db-spec (pg/event-log-spec @config/settings org-id)
        cdb-spec (cartodb-spec config org-id)
        offset (get-offset cdb-spec org-id)
        {:keys [chan close!] :as events} (pg/event-chan* db-spec offset)
        event-handler (wrap-update-offset config
                                          org-id
                                          event-handler)]
    (async/thread
      (loop []
        (when-let [event (async/<!! chan)]
          (event-handler event)
          (recur))))
    close!))

(defn restart [config org-id event-handler]
  (let [cdb-spec (cartodb-spec config org-id)]
    (queryf cdb-spec "DELETE FROM event_offset")
    (queryf cdb-spec "DELETE FROM question")
    (queryf cdb-spec "DELETE FROM survey")
    (queryf cdb-spec "DELETE FROM form")
    (queryf cdb-spec "DELETE FROM data_point")
    (delete-all-raw-data-tables cdb-spec)
    (start config org-id event-handler)))



(defn -main [start-or-restart config-file & instances]
  (let [cartodb-consumer (if (= "restart" start-or-restart)
                           restart
                           start)
        config (edn/read-string (slurp config-file))]))



(def create-entity-store-sql
  "CREATE TABLE IF NOT EXISTS entity_store (
     entity_type TEXT NOT NULL,
     id BIGINT NOT NULL,
     entity TEXT NOT NULL,
     PRIMARY KEY (entity_type, id)
   );
   DELETE FROM entity_store;")

(defn cartodb-entity-store [cdb-spec]
  (queryf cdb-spec create-entity-store-sql)
  (reify entity-store/IEntityStore
      (-get [_ entity-type id]
        (-> (queryf cdb-spec
                "SELECT entity FROM entity_store WHERE id=%s AND entity_type='%s'"
                id
                entity-type)
            first
            (get "entity")
            edn/read-string))
      (-set [_ entity-type id entity]
        ;; TODO postgresql 9.5 will have better upsert support
        (try (queryf cdb-spec
                     "INSERT INTO entity_store VALUES ('%s', %s, '%s');"
                     entity-type
                     id
                     (pr-str entity))
             (catch Exception e
               (queryf cdb-spec
                       "UPDATE entity_store SET entity='%s' WHERE id=%s AND entity_type='%s';"
                       (pr-str entity)
                       id
                       entity-type))))
      (-del [_ entity-type id]
        (queryf cdb-spec
                "DELETE FROM entity_store WHERE id=%s AND entity_type='%s'"
                id
                entity-type))))


(comment

  (let [config (edn/read-string (slurp "cartodb-config.edn"))]
    (config/set-settings! "cartodb-config.edn")
    (config/set-config! (@config/settings :config-folder)))


  (queryf cdb-spec
          "SELECT * FROM event_offset")

  (get @config/configs "flowaglimmerofhope-hrd")

  (:api-key env) env
  (def cdb-spec (cartodb-spec @config/configs "akvoflow-uat1"))


  (setup-tables cdb-spec)

  (get-offset cdb-spec "akvoflow-uat1")

  (def close! (restart @config/configs
                       "flowaglimmerofhope-hrd"
                       handle-event))
  (def close! (start @config/configs
                     "flowaglimmerofhope-hrd"
                     handle-event))

  (queryf cdb-spec cartodbfy-data-points)
  (close!)

  @config/settings




  ((:close! ch))



  (queryf cdb-spec "SELECT * FROM data_point")
  (queryf cdb-spec cartodbfy-data-points)


  (count
   (->> (queryf cdb-spec "SELECT tablename FROM pg_tables")
    (map #(get % "tablename"))
    (filter #(not (.startsWith % "pg_")))))


  (Thread/setDefaultUncaughtExceptionHandler
   (reify Thread$UncaughtExceptionHandler
     (uncaughtException [this thread e]
       (.printStackTrace e))))

  )




(comment

  ;; Entity Store Demo

  ;; Load config
  (let [config (edn/read-string (slurp "cartodb-config.edn"))]
    (config/set-settings! "cartodb-config.edn")
    (config/set-config! (@config/settings :config-folder)))

  ;; cartodb spec
  (def cdb-spec (cartodb-spec @config/configs "akvoflow-uat1"))

  ;; Create an entity-store for cartodb
  (def entity-store (cartodb-entity-store cdb-spec))
  ;; (def entity-store (entity-store/cached-entity-store (cartodb-entity-store cdb-spec) 2))

  ;; Add some entities to the store
  (entity-store/set-entity entity-store {"entityType" "Answer"
                                         "id" 1
                                         "foo" "A"})

  (entity-store/set-entity entity-store {"entityType" "Answer"
                                         "id" 2
                                         "foo" "B2"})

  (entity-store/set-entity entity-store {"entityType" "Answer"
                                         "id" 3
                                         "foo" "C"})

  ;; Get

  (entity-store/get-entity entity-store "Answer" 2)

  ;; Delete
  (entity-store/delete-entity entity-store "Answer" 2)






  )
