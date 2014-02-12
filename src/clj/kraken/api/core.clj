(ns kraken.api.core)

(def api-domain (atom "https://api.kraken.com/"))

(defn api-url [x & rest] (apply str @api-domain x rest))

(defn param-string [key-map]
  (apply str (interpose "&"
                        (map #(str (name (key %)) "=" (val %)) key-map))))

(defn url-with-params [url key-map]
  (str url "?" (param-string key-map)))



