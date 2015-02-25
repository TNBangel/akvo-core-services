(ns akvo-unified-log.json
  (:require [clojure.java.io :as io])
  (:import [org.postgresql.util PGobject]
           [com.github.fge.jsonschema.main JsonSchema JsonSchemaFactory]
           [com.fasterxml.jackson.databind JsonNode]
           [com.fasterxml.jackson.databind ObjectMapper]))

(def object-mapper (ObjectMapper.))

(defn json-node [s]
  (.readValue object-mapper s JsonNode))

(def schema-validator
  (.getJsonSchema (JsonSchemaFactory/byDefault)
                  (-> "../flow-data-schema/schema/event.json" io/file .toURI str)))

(defn valid? [json-node]
  (.validInstance schema-validator json-node))

(defn jsonb
  "Create a JSONB object"
  [s]
  (doto (PGobject.)
    (.setType "jsonb")
    (.setValue s)))
