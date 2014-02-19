(ns kraken.elastic
  (:use [kraken.system]
        [kraken.model])
  (:require [clojurewerkz.elastisch.rest.index :as esi]
            [clojurewerkz.elastisch.rest.document :as esd]
            [clojurewerkz.elastisch.rest :as esr]
            [clojurewerkz.elastisch.rest.bulk :as esb]
            [clojurewerkz.elastisch.query :as esq]
            [clj-time.core :as tcore]
            [clj-time.format :as tformat]
            [clj-time.coerce :as tcoerce]
            [clojure.core.async :as as]))

;; (get-cfg (system) :elastic))

(defn connect [system component-id]
  (let [conn (get-cfg system component-id :connection)]
    (if (= :rest (:type conn))
      (assoc-in system (flatten [component-id :connection :instance])
                (esr/connect (str (:protocol conn) "://" (:host conn) ":" (:port conn))))
      (throw (Exception. (str "Unknown connection type " (:type conn)))))))
;; (connect @+system+ :elastic)

(defmacro with-connection [[c con] & body]
  `(binding [esr/*endpoint* ~con]
     ~@body))

(defn system-connection [system component-id]
  (get-cfg system component-id :connection :instance))

(defmacro with-system-connection [[sc system component-id] & body]
  `(with-connection [sc (system-connection ~system ~component-id)]
     ~@body))

(defmulti querify (fn [x] (type x)))
(defmethod querify :default [x] 
  (if (coll? x) 
    (reduce conj (empty x) (map querify x))
    x))
(defmethod querify org.joda.time.DateTime [x]
  (tcoerce/to-long x))
(defmethod querify clojure.lang.MapEntry [x]
  [(querify (key x)) (querify (val x))])

(defn- index-document! [system component-id d]
  (with-system-connection [sc system component-id]
    (esd/create (index d system) (mapping-type d system) 
                (querify (document d system))))
  system)

(defn index! [system component-id d]
  (if (satisfies? DocumentP d)
    (index-document! system component-id d)
    (error system component-id (str d " does not satisfy the Document protocol."))))

(defn start-index-channel! [system component-id]
  (let [in (as/chan)]
    (as/go (loop [doc (as/<! in)]
             (if doc
               (do (index! system component-id doc)
                   (recur (as/<! in)))
               (as/close! in))))
    (set-cfg system component-id {:index-channel in})))

(defn init-elastic [system id]
  (-> system
      (connect id)
      (start-index-channel! id)))

(defn shutdown-elastic [system id]
  (when-let [in (get-cfg system id :index-channel)]
    (as/close! in))
  (set-cfg system id {:index-channel nil}))

(defcomponent :elastic []  
  ComponentP 
  (initial-config [this] (read-string (slurp (str (System/getProperty "user.home") "/.kraken/elastic/config.edn"))))
  (initialize [this system] (init-elastic system :elastic))
  (start [this system] system)
  (stop [this system] system)
  (shutdown [this system] (shutdown-elastic system :elastic)))

(defn create-index! [system component-id idx]
  (with-system-connection [sc system component-id]
    (when-not (esi/exists? idx)
      (info system component-id (str "Create index " idx ", settings " (get-cfg system :elastic :index-settings) ", mappings " (get-cfg system :elastic :mappings)))
      (esi/create idx
                  :settings (get-cfg system component-id :index-settings)
                  :mappings (get-cfg system component-id :mappings))))
  system)

;; (def idx (index (mk-trade "cryptsy" "DOGE/BTC" 0 0 0) (system)))
;; (create-index! (system) idx)
;; (get-cfg (system) :elastic)
;; (esd/count idx :trade)
;; 10

(defn hits [system idx mapping
            & {:keys [query filter size facets search_type sort fields]
               :or {query {:match_all {}}
                    search_type "query_then_fetch"}
               :as opts}]
  (with-system-connection [sc system :elastic]
    (let [esr (apply esd/search idx mapping 
                     (mapcat identity (merge opts {:query query :search_type search_type })))]
      (with-meta (map (fn [hit] (with-meta (or (:_source hit) (:fields hit))
                                  (dissoc hit :_source :fields)))
                      (:hits (:hits esr)))
        (merge (dissoc esr :hits)
               (dissoc (:hits esr) :hits))))))

(defn trades [system exchange-code
              & {:keys [query filter size facets search_type sort fields]
                 :as opts}]
  (let [hits (apply hits system (index-name system exchange-code) "trade"
                    (flatten (seq opts)))]
    (with-meta (map #(with-meta (mk-trade exchange-code 
                                          (:market-code %)
                                          (:price %)
                                          (:volume %)
                                          (tcoerce/from-long (:time %))
                                          (:id %))
                       (meta %))
                    hits)
      (meta hits))))

;; ;;; average trade prices:
;; (require '[incanter [charts :as ichart]
;;                     [core :as icore]])

;; (def esr (esd/search (index-name (system) "cryptsy") :trade
;;                      :search_type "count"
;;                      :facets {:ltp {:date_histogram {:key_field :time
;;                                                      :value_field :price
;;                                                      :interval "2m"}}}))
;; (def ats (:entries (:ltp (:facets esr))))
;; (icore/view (ichart/time-series-plot (map #(tcoerce/to-long (:time %)) ats)
;;                                      (map :mean ats)
;;                                      :x-label "Time"
;;                                      :y-label "Last trade price"
;;                                      :title "DOGE/BTC @ Cryptsy"))
;; ;;; volumes?
;; (def esr (esd/search (index-name (system) "cryptsy") :trade
;;                      :search_type "count"
;;                      :facets {:ltp {:date_histogram {:key_field :time
;;                                                      :value_field :volume
;;                                                      :interval "2m"}}}))
;; (def ats (:entries (:ltp (:facets esr))))
;; (icore/view (ichart/time-series-plot (map #(tcoerce/to-long (:time %)) ats)
;;                                      (map :mean ats)
;;                                      :x-label "Time"
;;                                      :y-label "Trade volume"
;;                                      :title "DOGE/BTC @ Cryptsy"))
;; ;;; we should 
;; esr
;; ;; 
;; ;; (with-system-connection [sc (system)]
;; ;;   (when-not (esi/exists? idx)
;; ;;     (esi/create "test/123"
;; ;;                 ;; idx
;; ;;                 ;; :settings {"number_of_shards" 1, "number_of_replicas" "1"}
;; ;;                 ;; :mappings {:trade
;; ;;                 ;;            {:properties
;; ;;                 ;;             {:price {:type "double"},
;; ;;                 ;;              :volume {:type "double"},
;; ;;                 ;;              :time {:type "date"}}}}
;; ;;                 )))

;; ;; ;;; I. General helper functions (connect/create/delete/scroll idx ..._)
;; ;; ;;; ===================================================================

;; ;; (defmacro with-connection [[con] & body]
;; ;;   `(binding [esr/*endpoint* ~con]
;; ;;      ~@body))

;; ;; (defn local-connection []
;; ;;   (esr/connect "http://localhost:9200"))

;; ;; (defn- lazy-scroll [scroll-id scroll]
;; ;;   (let [response (esd/scroll scroll-id :scroll scroll)]
;; ;;     (if (> (count (:hits (:hits response))) 0)
;; ;;       (lazy-cat (map (fn [hit] (with-meta (or (:_source hit) (:fields hit)) 
;; ;;                                  (dissoc hit :_source :fields)))
;; ;;                      (:hits (:hits response)))
;; ;;                 (lazy-scroll (:_scroll_id response) scroll))
;; ;;       '())))

;; ;; (defn hit-seq [es-connection idx mapping
;; ;;                & {:keys [query filter size scroll fields]
;; ;;                   :or {query {:match_all {}}
;; ;;                        scroll "1m"
;; ;;                        size 2000}
;; ;;                   :as opts}]
;; ;;   (with-connection [es-connection]
;; ;;     (let [initial-response (apply esd/search idx mapping 
;; ;;                                   (mapcat identity (assoc opts
;; ;;                                                      :query query
;; ;;                                                      :scroll scroll
;; ;;                                                      :search_type "scan"
;; ;;                                                      :size size)))]
;; ;;       (with-meta (lazy-scroll (:_scroll_id initial-response) scroll)
;; ;;         (merge (dissoc initial-response :hits)
;; ;;                (dissoc (:hits initial-response) :hits))))))


;; ;; ;;; II. Queries construction helper functions
;; ;; ;;; =========================================

;; ;; (defmulti querify (fn [x] (type x)))
;; ;; (defmethod querify :default [x] 
;; ;;   (if (coll? x) (into (empty x) (map querify x))))
;; ;; (defmethod querify org.joda.time.DateTime [x]
;; ;;   (tcoerce/to-long x))

;; ;; (defn match-all-query []
;; ;;   (esq/match-all))

;; ;; (defn filtered-query [& {:keys [query filter] :or {query (match-all-query)}}]
;; ;;   (esq/filtered :filter filter :query (querify query)))

;; ;; (defn term-query [field value & {:keys [boost] :or {boost 1}}]
;; ;;   {:term {field {:value (querify value) :boost boost}}})

;; ;; (defn match-query [field to-match & {:keys [operator zero-terms-query] 
;; ;;                                      :or {operator "and", zero-terms-query "none"}}]
;; ;;   {:match {field {:query (querify to-match)
;; ;;                   :operator operator
;; ;;                   :zero_terms_query zero-terms-query}}})

;; ;; (defn query-string-query [query-string & {:keys [default-field default-operator allow-leading-wildcard lowercase-expanded-terms enable-position-increments fuzzy-max-expansions
;; ;;                                                  fuzzy-min-sim fuzzy-prefix-length phrase-slop boost analyze-wildcard auto-generate-phrase-queries minimum-should-match lenient ]
;; ;;                                           :or {default-field "_all"
;; ;;                                                default-operator "OR"
;; ;;                                                allow-leading-wildcard true
;; ;;                                                lowercase-expanded-terms true
;; ;;                                                enable-position-increments true
;; ;;                                                fuzzy-max-expansions 50
;; ;;                                                fuzzy-min-sim 0.5
;; ;;                                                fuzzy-prefix-length 0
;; ;;                                                phrase-slop 0
;; ;;                                                boost 1.0
;; ;;                                                analyze-wildcard false
;; ;;                                                auto-generate-phrase-queries false
;; ;;                                                minimum-should-match "100%"
;; ;;                                                lenient true}}]
;; ;;   {:query_string {:query query-string
;; ;;                   :default_field default-field
;; ;;                   :default_operator default-operator
;; ;;                   :allow_leading_wildcard allow-leading-wildcard 
;; ;;                   :lowercase_expanded_terms lowercase-expanded-terms 
;; ;;                   :enable_position_increments enable-position-increments 
;; ;;                   :fuzzy_max_expansions fuzzy-max-expansions 
;; ;;                   :fuzzy_min_sim fuzzy-min-sim
;; ;;                   :fuzzy_prefix_length fuzzy-prefix-length 
;; ;;                   :phrase_slop phrase-slop
;; ;;                   :boost boost
;; ;;                   :analyze_wildcard analyze-wildcard 
;; ;;                   :auto_generate_phrase_queries auto-generate-phrase-queries
;; ;;                   :minimum_should_match minimum-should-match
;; ;;                   :lenient lenient}})

;; ;; ;;; III. Filter construction helper functions
;; ;; ;;; ========================================

;; ;; (defn term-filter [field value & {:keys [cache] :or {cache true}}]
;; ;;   {:term {field (querify value) :_cache cache}})

;; ;; (defn and-filter [& filters] 
;; ;;   (when filters
;; ;;     {:and {:filters (vec filters)
;; ;;            :_cache false}}))

;; ;; (defn not-filter [filter-or-query & {:keys [cache] :or {cache false}}]
;; ;;   {:not {:filter filter-or-query :_cache cache}})

;; ;; (defn query-filter [query]
;; ;;   {:query query})

;; ;; (defn daterange-filter 
;; ;;   ([field from-date]
;; ;;      {:range {field {:gte (tformat/unparse (tformat/formatters :date-hour-minute-second)
;; ;;                                            from-date)}}})
;; ;;   ([field from-date to-date]
;; ;;      {:range {field {:gte (tformat/unparse (tformat/formatters :date-hour-minute-second)
;; ;;                                            from-date)
;; ;;                      :lte (tformat/unparse (tformat/formatters :date-hour-minute-second)
;; ;;                                            to-date)}}}))


;; ;; ;;; IV. Cleansing helper functions
;; ;; ;;; ===============================


;; ;; (defn compute-duplicates [es-connection idx mapping]
;; ;;   "Returns a sequence of all entries in idx/mapping that are identical to another entry
;; ;; content-wise. If there are N identical entries, then only N-1 entries are returned (so that it is
;; ;; safe to simply delete all entries returned)."
;; ;;   (let [all-results (result-seq es-connection idx mapping)
;; ;;         grps (group-by identity all-results)]
;; ;;     (mapcat rest (vals grps))))

;; ;; (defn delete-duplicates [es-connection idx mapping]
;; ;;   "See compute-duplicates."
;; ;;   (let [duplicates (compute-duplicates es-connection idx mapping)]
;; ;;     (with-connection [es-connection]    
;; ;;       (esb/bulk (esb/bulk-index (map meta duplicates))))))

;; ;; ;;; V. Kraken specific stuff
;; ;; ;;; ========================


;; ;; (defn create-indices 
;; ;;   ([es-connection] (create-indices es-connection [:tick :spread :trade]))
;; ;;   ([es-connection indices]
;; ;;      (with-connection [es-connection]
;; ;;        (doseq [idx indices]
;; ;;          (when-not (esi/exists? idx)
;; ;;            (esi/create idx 
;; ;;                        :mappings {idx (get mappings idx)}))))))

;; ;; (defn delete-indices 
;; ;;   ([es-connection] (delete-indices es-connection [:tick :spread :trade]))
;; ;;   ([es-connection indices]
;; ;;      (with-connection [es-connection]
;; ;;        (doseq [idx indices]
;; ;;          (when (esi/exists? idx)
;; ;;            (esi/delete idx))))))


;; ;; ;;; Ticks

;; ;; (defn index-tick 
;; ;;   ([es-connection tick]
;; ;;      (with-connection [es-connection]
;; ;;        (esd/create :tick :tick 
;; ;;                    (update-in tick [:time] tcoerce/to-date))))
;; ;;   ([es-connection tick & ticks]
;; ;;      (with-connection [es-connection]
;; ;;        (esb/bulk (esb/bulk-index (map #(assoc % :_index :tick :_type :tick :time (tcoerce/to-date (:time %))) (conj ticks tick)))))))
;; ;; (defn- result->tick [result]
;; ;;   (let [result (update-in result [:time] tformat/parse)]
;; ;;     (with-meta (apply mk-tick (map #(get result %) [:time :asset :ask-price :ask-volume :bid-price :bid-volume :last-price :last-volume :volume24 :trades24 :low24 :high24 :opening]))
;; ;;       (meta result))))
;; ;; (defn ticks [es-connection
;; ;;              & {:keys [query filter size facets search_type sort fields]
;; ;;                 :or {query {:match_all {}}
;; ;;                      search-type "query_then_fetch"
;; ;;                      ;; sort [{:time {:order "desc"}}]
;; ;;                      }
;; ;;                 :as opts}]
;; ;;   (let [results (apply results es-connection :tick :tick (mapcat identity opts))]
;; ;;     (with-meta (map result->tick results)
;; ;;       (meta results))))
;; ;; (defn all-ticks [es-connection 
;; ;;                  & {:keys [query filter size fields]
;; ;;                     :or {query {:match_all {}}}
;; ;;                     :as opts}]
;; ;;   (let [results (apply result-seq es-connection :tick :tick (mapcat identity (merge opts {:query query})))]
;; ;;     (with-meta (map result->tick results)
;; ;;       (meta results))))

;; ;; ;;; Spreads

;; ;; (defn index-spread 
;; ;;   ([es-connection spread]
;; ;;      (with-connection [es-connection]
;; ;;        (esd/create :spread :spread 
;; ;;                    (update-in spread [:time] tcoerce/to-date))))
;; ;;   ([es-connection spread1 spread2 & spreads]
;; ;;      (with-connection [es-connection]
;; ;;        (esb/bulk-with-index-and-type :spread :spread
;; ;;                                      (esb/bulk-index (map #(assoc % :time (tcoerce/to-date (:time %)))
;; ;;                                                           (conj spreads spread2 spread1)))))))
;; ;; (defn- result->spread [result]
;; ;;   (let [result (update-in result [:time] tformat/parse)]
;; ;;     (with-meta (apply mk-spread (map #(get result %) [:bid :ask :time :asset]))
;; ;;       (meta result))))
;; ;; (defn spreads [es-connection
;; ;;                & {:keys [query filter size facets search_type sort fields]
;; ;;                   :or {query {:match_all {}}
;; ;;                        search-type "query_then_fetch"
;; ;;                        ;; sort [{:time {:order "desc"}}]
;; ;;                        }
;; ;;                   :as opts}]
;; ;;   (let [results (apply results es-connection :spread :spread (mapcat identity opts))]
;; ;;     (with-meta (map result->spread results)
;; ;;       (meta results))))
;; ;; (defn all-spreads [es-connection 
;; ;;                    & {:keys [query filter size fields]
;; ;;                       :or {query {:match_all {}}}
;; ;;                       :as opts}]
;; ;;   (let [results (apply result-seq es-connection :spread :spread (mapcat identity (merge opts {:query query})))]
;; ;;     (with-meta (map result->spread results)
;; ;;       (meta results))))

;; ;; ;;; Trades

;; ;; (defn index-trade 
;; ;;   ([es-connection trade]
;; ;;      (with-connection [es-connection]
;; ;;        (esd/create :trade :trade 
;; ;;                    (update-in trade [:time] tcoerce/to-date))))
;; ;;   ([es-connection trade1 trade2  & trades]
;; ;;      (with-connection [es-connection]
;; ;;        (esb/bulk-with-index-and-type :trade :trade
;; ;;                                      (esb/bulk-index (map #(assoc % :time (tcoerce/to-date (:time %))) (conj trades trade2 trade1)))))))

;; ;; (defn- result->trade [result]
;; ;;   (let [result (update-in result [:time] tformat/parse)]
;; ;;     (with-meta (apply mk-trade (map #(get result %) [:price :volume :time :bid-type :order-type :misc :asset]))
;; ;;       (meta result))))
;; ;; (defn trades [es-connection 
;; ;;               & {:keys [query filter size facets search_type sort fields]
;; ;;                  :or {query {:match_all {}}
;; ;;                       search-type "query_then_fetch"}
;; ;;                  :as opts}]
;; ;;   (let [results (apply results es-connection :trade :trade (mapcat identity opts))]
;; ;;     (with-meta (map result->trade results)
;; ;;       (meta results))))
;; ;; (defn all-trades [es-connection 
;; ;;                   & {:keys [query filter size fields]
;; ;;                      :or {query {:match_all {}}}
;; ;;                      :as opts}]
;; ;;   (let [results (apply result-seq es-connection :trade :trade (mapcat identity (merge opts {:query query})))]
;; ;;     (with-meta (map result->trade results)
;; ;;       (meta results))))


