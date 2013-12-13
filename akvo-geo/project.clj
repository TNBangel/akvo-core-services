(defproject akvo-geo "0.1.0"
  :description "HTTP service that performs reverse geocoding based on the GAUL database"
  :url "https://github.com/akvo/akvo-flow-geoservice"
  :license {:name "GNU Affero General Public License"
            :url "https://www.gnu.org/licenses/agpl"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [korma "0.3.0-RC5"]
                 [org.postgresql/postgresql "9.2-1002-jdbc4"]
                 [liberator "0.10.0"]
                 [cheshire "5.1.1"]
                 [compojure "1.1.5"]
                 [ring/ring-core "1.1.8"]
                 [ring/ring-jetty-adapter "1.1.8"]
                 [org.slf4j/slf4j-api "1.7.3"]
                 [org.slf4j/slf4j-simple "1.7.3"]]
  :main akvo-geo.core
  :aot [akvo-geo.core]
  :plugins [[lein-ring "0.8.5"]]
  :ring {:handler akvo-geo.core/app
         :init akvo-geo.core/init})
