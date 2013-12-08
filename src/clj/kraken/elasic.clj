(ns kraken.elastic
  (:use [kraken.model])
  (:require [clojurewerkz.elastisch.rest.index :as esi]
            [clojurewerkz.elastisch.rest.document :as esd]
            [clojurewerkz.elastisch.rest :as esr]
            [clojurewerkz.elastisch.query :as esq]
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

(defn ticks [es-connection
             & {:keys [query filter size facets search_type sort]
                :or {query {:match_all {}}
                     search-type "query_then_fetch"
                     ;; sort [{:time {:order "desc"}}]
                     }
                :as opts}]
  (with-connection [es-connection]
    (apply esd/search :tick :tick 
           (mapcat identity (merge {:query {:match_all {}} 
                                    :search_type "query_then_fetch"}
                                   opts)))))

(defn index-spread [es-connection spread]
  (with-connection [es-connection]
    (esd/create :spread :spread 
                (update-in spread [:time] tcoerce/to-date))))

(defn spreads [es-connection
               & {:keys [query filter size facets search_type sort]
                  :or {query {:match_all {}}
                       search-type "query_then_fetch"
                       ;; sort [{:time {:order "desc"}}]
                       }
                  :as opts}]
  (with-connection [es-connection]
    (apply esd/search :spread :spread 
           (mapcat identity (merge {:query {:match_all {}} 
                                    :search_type "query_then_fetch"}
                                   opts)))))

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

(defn trades [es-connection 
              & {:keys [query filter size facets search_type sort]
                  :or {query {:match_all {}}
                       search-type "query_then_fetch"
                       sort [{:time {:order "desc"}}]}
                  :as opts}]
  (with-connection 
    (apply esd/search :trade :trade 
           (mapcat identity (merge {:query {:match_all {}} 
                                    :search_type "query_then_fetch"
                                    :sort [{:time {:order "desc"}}] }
                                   opts)))))
;; (def spread (first (kraken.api.public/spread "LTCEUR")))

;; (with-connection [(local-connection)]
;;   (esi/exists? :tick))

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
;; (with-connection [(local-connection)] (esd/search :tick :tick :query {:match_all {}} :size 1))
;; (with-connection [(local-connection)] (esd/count :tick :tick {:match_all {}}))

;; (with-connection [(local-connection)] (esd/search :kraken-ticks :tick :query {:match_all {}} :size 3 :sort [{:time {:order "desc"}}]))

;; (with-connection [(local-connection)] (map :_source (:hits (:hits (esd/search :kraken-ticks :tick :query {:match_all {}} :size 3 :sort [{:time {:order "desc"}}]))))


;;;;;;; Some helper functions for creating elastic queries and filters


;;; Queries 

(defn match-all-query []
  (esq/match-all))

(defn filtered-query [& {:keys [query filter] :or {query (match-all-query)}}]
  (esq/filtered :filter filter :query query))

(defn term-query [field value & {:keys [boost] :or {boost 1}}]
  {:term {field {:value value :boost boost}}})

(defn match-query [field to-match & {:keys [operator zero-terms-query] 
                                     :or {operator "and", zero-terms-query "none"}}]
  {:match {field {:query to-match
                  :operator operator
                  :zero_terms_query zero-terms-query}}})

(defn query-string-query [query-string & {:keys [default-field default-operator allow-leading-wildcard lowercase-expanded-terms enable-position-increments fuzzy-max-expansions
                                                 fuzzy-min-sim fuzzy-prefix-length phrase-slop boost analyze-wildcard auto-generate-phrase-queries minimum-should-match lenient ]
                                          :or {default-field "_all"
                                               default-operator "OR"
                                               allow-leading-wildcard true
                                               lowercase-expanded-terms true
                                               enable-position-increments true
                                               fuzzy-max-expansions 50
                                               fuzzy-min-sim 0.5
                                               fuzzy-prefix-length 0
                                               phrase-slop 0
                                               boost 1.0
                                               analyze-wildcard false
                                               auto-generate-phrase-queries false
                                               minimum-should-match "100%"
                                               lenient true}}]
  {:query_string {:query query-string
                  :default_field default-field
                  :default_operator default-operator
                  :allow_leading_wildcard allow-leading-wildcard 
                  :lowercase_expanded_terms lowercase-expanded-terms 
                  :enable_position_increments enable-position-increments 
                  :fuzzy_max_expansions fuzzy-max-expansions 
                  :fuzzy_min_sim fuzzy-min-sim
                  :fuzzy_prefix_length fuzzy-prefix-length 
                  :phrase_slop phrase-slop
                  :boost boost
                  :analyze_wildcard analyze-wildcard 
                  :auto_generate_phrase_queries auto-generate-phrase-queries
                  :minimum_should_match minimum-should-match
                  :lenient lenient}})

;;; Filters

(defn term-filter [field value & {:keys [cache] :or {cache true}}]
  {:term {field value :_cache cache}})

(defn and-filter [& filters] 
  (when filters
    {:and {:filters (vec filters)
           :_cache false}}))

(defn not-filter [filter-or-query & {:keys [cache] :or {cache false}}]
  {:not {:filter filter-or-query :_cache cache}})

(defn query-filter [query]
  {:query query})

(defn daterange-filter 
  ([field from-date]
     {:range {field {:gte (tformat/unparse (tformat/formatters :date-hour-minute-second)
                                           from-date)}}})
  ([field from-date to-date]
     {:range {field {:gte (tformat/unparse (tformat/formatters :date-hour-minute-second)
                                           from-date)
                     :lte (tformat/unparse (tformat/formatters :date-hour-minute-second)
                                           to-date)}}}))

