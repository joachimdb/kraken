(ns kraken.elastic
  (:use [kraken.system])
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

(defprotocol DocumentP
  (index [this system])
  (mapping-type [this system])
  (document [this system]))

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

(defn terms
  [system idx mapping field
   & {:keys [query filter all-terms exclude size order]
      :or {query {:match_all {}}
           all-terms "false"
           order "count"
           size 10
           exclude []}
      :as opts}]
  (with-system-connection [sc system :elastic]
    (let [esr (esd/search idx mapping 
                          :query query
                          :search_type "count"
                          :facets {field {:terms {:field field :size size :order order :all_terms all-terms :exclude exclude}}})]
      (with-meta (get-in esr [:facets field :terms]) 
        (dissoc (get-in esr [:facets field]) :terms)))))


(defn date-histogram
  [system idx mapping key-field value-field interval
   & {:keys [query filter time_zone factor pre_offset post_offset]
      :or {query {:match_all {}}}
      :as opts}]
  (with-system-connection [sc system :elastic]
    (let [esr (esd/search idx mapping 
                          :query query
                          :search_type "count"
                          :facets 
                          (deep-merge {:dh {:date_histogram (assoc (select-keys opts [:time_zone :factor :pre_offset :post_offset])
                                                              :key_field key-field
                                                              :value_field value-field
                                                              :interval interval)}}
                                      (if filter
                                        {:dh {:facet_filter filter}}
                                        {})))]
      (with-meta (map #(update-in % [key-field] tcoerce/from-long)
                      (get-in esr [:facets :dh :entries])) 
        (dissoc (get-in esr [:facets :dh]) :entries)))))

