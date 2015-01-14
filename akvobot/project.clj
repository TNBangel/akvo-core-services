(defproject akvobot "0.0.1"
  :description "Small bot to aid the development team"
  :url "http://akvo.org"
  :license {:name "Affero General Public License"
            :url "https://gnu.org/licenses/agpl.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [ring/ring-core "1.3.2"]
                 [ring/ring-json "0.3.1"]
                 [ring/ring-jetty-adapter "1.3.2"]
                 [compojure "1.3.1"]
                 [tentacles "0.3.0"]
                 [environ "1.0.0"]]
  :profiles {:dev {:dependencies [[javax.servlet/servlet-api "2.5"]]}})
