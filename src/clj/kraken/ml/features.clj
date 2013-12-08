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

;; last XLTCZEUR tick:
(ticks (local-connection)
       :query (match-query :asset "XLTCZEUR")
       :sort [{:time {:order "desc"}}]
       :size 1)

;; first XLTCZEUR tick
(ticks (local-connection)
       :query (match-query :asset "XLTCZEUR")
       :sort [{:time {:order "asc"}}]
       :size 1)

;; count XLTCZEUR ticks:
(ticks (local-connection)
       :query (filtered-query :query (match-query :asset "XLTCZEUR")
                              :filter (daterange-filter :time (tcore/ago (tcore/days 1))))
       :search_type "count")

;; date hostogram over :last-price field of XLTCZEUR ticks:
(ticks (local-connection)
       :query (filtered-query :query (match-query :asset "XLTCZEUR")
                              :filter (daterange-filter :time (tcore/ago (tcore/hours 1))))
       :facets {:price-last-hour
                {:date_histogram {:key_field :time
                                  :value_field :last-price
                                  :interval "5m"}}}
       :search_type "count")
