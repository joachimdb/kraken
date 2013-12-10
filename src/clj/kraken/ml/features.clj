(ns kraken.ml.features
  (:use [kraken.model]
        [kraken.elastic])
  (:require [clj-time.core :as tcore]
            [clj-time.format :as tformat]
            [clj-time.coerce :as tcoerce]))


;;; Some helper functions to extract features from elastic (wip)

(ticks (local-connection)
       :query {:match_all {}}
       :size 1)

;; all ticks from the last half our in descending order:
(ticks (local-connection)
       :query (filtered-query :query (daterange-filter :time (tcore/ago (tcore/minutes 30))))
       :sort [{:time {:order "desc"}}])

;; all XLTCZEUR ticks from the last half our in descending order:
(ticks (local-connection)
       :query (match-query :asset "XLTCZEUR")
       :sort [{:time {:order "desc"}}])


;; first XLTCZEUR tick
(ticks (local-connection)
       :query (match-query :asset "XLTCZEUR")
       :sort [{:time {:order "asc"}}]
       :size 1)


;; 
(spreads (local-connection)
         :search_type "count") ;; 27511

(trades (local-connection)
        :search_type "count") ;; 10820

;; date hostogram over :last-price field of XLTCZEUR ticks:
(ticks (local-connection)
       :query (filtered-query :query (match-query :asset "XLTCZEUR")
                              :filter (daterange-filter :time (tcore/ago (tcore/hours 1))))
       :facets {:price-last-hour
                {:date_histogram {:key_field :time
                                  :value_field :last-price
                                  :interval "5m"}}}
       :search_type "count")


;; Check for and remove duplicate entries

(count (compute-duplicates (local-connection) :spread :spread))
(delete-duplicates (local-connection) :spread :spread)

(count (compute-duplicates (local-connection) :trade :trade))
(delete-duplicates (local-connection) :trade :trade)

(trades (local-connection) :search_type "count")
(ticks (local-connection) :search_type "count")
(spreads (local-connection) :search_type "count")

(def ts (with-open [rdr (clojure.java.io/reader "/tmp/ticks.edn")]
           (doall (map read-string (line-seq rdr)))))
(doseq [t ts]
  (doseq [[pair tick-data] (dissoc t :lt)]
    (index-tick (local-connection) 
                (mk-tick (tformat/parse (:lt t))
                         (name pair)
                         (double (read-string (first (get-in tick-data [:a]))))
                         (double (read-string (second (get-in tick-data [:a]))))
                         (double (read-string (first (get-in tick-data [:b]))))
                         (double (read-string (second (get-in tick-data [:b]))))
                         (double (read-string (first (get-in tick-data [:c]))))
                         (double (read-string (second (get-in tick-data [:c]))))
                         (double (read-string (first (get-in tick-data [:v]))))
                         (long (first (get-in tick-data [:t])))
                         (double (read-string (first (get-in tick-data [:l]))))
                         (double (read-string (first (get-in tick-data [:h]))))
                         (double (read-string (get-in tick-data [:o])))))))

(ticks (local-connection) :search_type "count")
;; 14949

(count ts)
(first ts)
(keys (group-by count ts))
(doseq saved-ticks [line-seq ])


;;; ticks

;;; n-ticks:
;; count XLTCZEUR ticks:
(ticks (local-connection)
       :query (filtered-query :query (match-query :asset "XLTCZEUR"))
       :search_type "count") ;; 67677
;;; last tick:
(ticks (local-connection)
       :query (match-query :asset "XLTCZEUR")
       :sort [{:time {:order "desc"}}]
       :size 1)
;; :last-price 23.599,
;; :time "2013-12-09T20:34:10Z",
(count (compute-duplicates (local-connection) :tick :tick)) ;; 0



;;; spreads

;;; n-spreads:
;; count XLTCZEUR ticks:
(meta (spreads (local-connection)
               :query (filtered-query :query (match-query :asset "XXBTZEUR"))
               :search_type "count")) ;; 3380
;;; last spread:
(ticks (local-connection)
       :query (match-query :asset "XLTCZEUR")
       :sort [{:time {:order "desc"}}]
       :size 1)
;; :last-price 23.599,
;; :time "2013-12-09T20:34:10Z",
(count (compute-duplicates (local-connection) :tick :tick)) ;; 0
