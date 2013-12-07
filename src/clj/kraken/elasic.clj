(ns kraken.elastic
  (:use [kraken.model])
  (:require [clojurewerkz.elastisch.rest.index :as esi]
            [clojurewerkz.elastisch.rest.document :as esd]
            [clojurewerkz.elastisch.rest :as esr]
            [clj-time.format :as tformat]
            [clj-time.coerce :as tcoerce]
            [clojure.core.async :as as]))

(defmacro with-connection [[con] & body]
  `(binding [esr/*endpoint* ~con]
     ~@body))

(defn local-connection []
  (esr/connect "http://localhost:9200"))

(defn create-indices 
  ([es-connection] (create-indices es-connection [:tick :spread :trade]))
  ([es-connection indices]
     (with-connection [es-connection]
       (doseq [idx indices]
         (when-not (esi/exists? idx)
           (esi/create idx 
                       :mappings {idx (get mappings idx)}))))))

(defn delete-indices 
  ([es-connection] (delete-indices es-connection [:tick :spread :trade]))
  ([es-connection indices]
     (with-connection [es-connection]
       (doseq [idx indices]
         (when (esi/exists? idx)
           (esi/delete idx)))))) 

(defn index-tick [es-connection tick]
  (with-connection [es-connection]
    (esd/create :tick :tick 
                (update-in tick [:time] tcoerce/to-date))))

(defn ticks [& {:keys [query filter size facets search_type sort]
                :or {query {:match_all {}}
                     search-type "query_then_fetch"
                     ;; sort [{:time {:order "desc"}}]
                     }
                :as opts}]
  (apply esd/search :tick :tick 
         (mapcat identity (merge {:query {:match_all {}} 
                                  :search_type "query_then_fetch"}
                                 opts))))

(defn index-spread [es-connection spread]
  (with-connection [es-connection]
    (esd/create :spread :spread 
                (update-in spread [:time] tcoerce/to-date))))

(defn spreads [& {:keys [query filter size facets search_type sort]
                  :or {query {:match_all {}}
                       search-type "query_then_fetch"
                       ;; sort [{:time {:order "desc"}}]
                       }
                  :as opts}]
  (apply esd/search :spread :spread 
         (mapcat identity (merge {:query {:match_all {}} 
                                  :search_type "query_then_fetch"}
                                 opts))))

(defn index-trade [es-connection trade]
  (with-connection [es-connection]
    (esd/create :trade :trade 
                (update-in trade [:time] tcoerce/to-date))))
(defn trades [& {:keys [query filter size facets search_type sort]
                 :or {query {:match_all {}}
                      search_type "query_then_fetch"
                      ;; sort [{:time {:order "desc"}}]
                      }
                 :as opts}]
  (apply esd/search :trade :trade 
         (mapcat identity (merge {:query {:match_all {}} 
                                  :search_type "query_then_fetch"}
                                 opts))))

(defn trades [& {:keys [query filter size facets search_type sort]
                  :or {query {:match_all {}}
                       search-type "query_then_fetch"
                       sort [{:time {:order "desc"}}]}
                  :as opts}]
  (apply esd/search :trade :trade 
         (mapcat identity (merge {:query {:match_all {}} 
                                  :search_type "query_then_fetch"
                                  :sort [{:time {:order "desc"}}] }
                                 opts))))
;; (def spread (first (kraken.api.public/spread "LTCEUR")))

;; (delete-indices (local-connection))
;; (create-indices (local-connection))

;; ;;; test
;; (def s (first (kraken.api.public/spread "LTCEUR")))
;; (with-connection [(local-connection)]
;;   (esd/create :spread :spread
;;               (update-in s [:time] tcoerce/to-date)))
;; (with-connection [(local-connection)] 
;;   (esd/search :spread :spread :query {:match_all {}}))

;; (delete-indices (local-connection) [:spread])
;; (create-indices (local-connection))

;; (with-connection [(local-connection)] (esd/count :tick :tick {:match_all {}}))
;; (with-connection [(local-connection)] (esd/count :tick :tick {:match_all {}}))

;; (with-connection [(local-connection)] (esd/search :kraken-ticks :tick :query {:match_all {}} :size 3 :sort [{:time {:order "desc"}}]))

;; (with-connection [(local-connection)] (map :_source (:hits (:hits (esd/search :kraken-ticks :tick :query {:match_all {}} :size 3 :sort [{:time {:order "desc"}}]))))
