(ns kraken.api.core)

(def api-domain "https://api.kraken.com/")

(defn api-url [x] (str api-domain x))
(defn public-url [x] (str api-domain "0/public/" x))

(defn param-string [key-map]
  (apply str (interpose "&"
                      (map #(str (name (key %)) "=" (val %)) key-map))))

(defn url-with-params [url key-map]
  (str url "?" (param-string key-map)))

