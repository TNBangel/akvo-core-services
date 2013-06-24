(defproject akvo-auth "0.1.0-SNAPSHOT"
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [compojure "1.1.5"]
                 [hiccup "1.0.3"]
                 [http-kit "2.1.4"]
                 [com.cemerick/friend "0.1.5"]
                 [ring/ring-core "1.1.8"]
                 [ring/ring-devel "1.1.8"]]
  :description "Akvo authentication and authorisation service"
  :license {:name "GNU Affero General Public License"
            :url "http://www.gnu.org/licenses/agpl.html"}
  :main akvo-auth.core
  :url "http://www.akvo.org/")
