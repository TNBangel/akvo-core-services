(ns akvo-auth.html
  (:require [hiccup [element :refer [link-to]]
                    [page :refer [html5 include-css]]]))

(defn- page [& content]
  (html5 {:lang "en"}
    [:head
     [:title "Akvo auth"]
     [:meta {:name "author" :content "Stichting Akvo // Akvo Foundation"}]
     [:meta {:name "description" :content "Akvo authentication service"}]
     [:meta {:name "keywords" :content "akvo,auth"}]
     [:link {:href "/favicon.ico" :rel "shortcut icon" :type "image/x-icon"}]
     (include-css "//netdna.bootstrapcdn.com/bootswatch/2.3.2/flatly/bootstrap.min.css"
                  "/styles.css")]
    [:body content]))

(defn- section [& content]
  [:section.container content])

(defn- row [& content]
  [:div.row content])

(defn- span [width & content]
  [:div {:class (str "span" width)} content])

(defn home []
  (page
    (section
      [:header [:h1 "Akvo auth"]]
      [:h3 "Sign up for an OpenID account with Akvo"])))

(defn not-found []
  (page
    (section
      [:h1 "Page not found"]
      [:p "The page you are looking for could not be found."]
      [:p "Go " (link-to "/" "home") "."])))
