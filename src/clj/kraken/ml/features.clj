(ns kraken.ml.features
  (:use [kraken.model]
        [kraken.elastic]
        [kraken.ml.vw]
        [kraken.view.charts]
        [incanter.core])
  (:require [clojurewerkz.elastisch.rest.bulk :as esb]
            [clj-time.core :as tcore]
            [clj-time.format :as tformat]
            [clj-time.coerce :as tcoerce]))


;;; We consider 3 labels: buy, sell and wait 
(def buy 0)
(def wait 1)
(def sell 2)

;;; 
(def forecast-time (tcore/hours 4))
(def focus-window-size (tcore/minutes 10))

;;; A datapoint is a "buy" if the last-price is significantly higher at forecast time.

(use 'incanter.core)
(use 'incanter.charts)

(def data (ticks (local-connection)
                 :query (filtered-query :query {:match {:asset "XLTCZEUR"}}
                                        :filter (daterange-filter :time (tcoerce/from-long 1386400000000)))
                 ;; :search_type "count"
                 :sort [{:time {:order "asc"}}]
                 :facets {:last-price {:date_histogram {:key_field :time
                                                        :value_field :last-price
                                                        :interval "20m"}}
                          :ask-price {:date_histogram {:key_field :time
                                                        :value_field :ask-price
                                                        :interval "20m"}}
                          :bid-price {:date_histogram {:key_field :time
                                                        :value_field :bid-price
                                                        :interval "20m"}}}
                 :size 10000))


(defn future-price [pair time interval]
  (let [ticks (ticks (local-connection)
                     :query (filtered-query :query {:match {:asset "XLTCZEUR"}}
                                            :fields :last-price
                                            :filter (daterange-filter :time 
                                                                      (tcore/minus time interval)
                                                                      (tcore/plus time interval))))]
    (avg (map :last-price ticks))))
(def fp (map #(- (future-price "XLTCZEUR" (tcore/plus (:time %) (tcore/hours 1)) (tcore/minutes 10))
                 (:last-price %))
             data))
(count fp)
(def fp (map #(if (< % 0) (Math/ceil %) (Math/floor %)) fp))a



(first data)
(def f (get-in (meta data) [:facets :last-price]))
(def a (get-in (meta data) [:facets :ask-price]))
(def b (get-in (meta data) [:facets :bid-price]))
(def start-date (tcoerce/from-long (:time (first (:entries f)))))  ;; #<DateTime 2013-12-07T10:50:00.000Z>
(def end-date (tcoerce/from-long (:time (last (:entries f)))))  ;;#<DateTime 2013-12-11T10:40:00.000Z>

(def ch (xy-plot (map :time (:entries f))
                 (map :mean (:entries f))))
(view ch)
(add-lines ch (map :time (:entries a)) (map :mean (:entries a)))
(add-lines ch (map :time (:entries b)) (map :mean (:entries b)))
(set-y-range ch 22 27)

;;; Idea: calculate predictions for several ranges. If all of them look good => buy. If all of them look bad => sell

;;; Predict 1h => avg of +50min->+70min
;;;         6h => avg of +5h and +7h
;;;        12h => avg of +11h and +13h
;;;        24h => ...




(defn avg [x] (when x (/ (reduce + x) (count x))))

(defn label [tick]
  (let [start-price (:last-price tick)
        end-price (avg (map :last-price 
                            (all-ticks (local-connection) 
                                       :query (match-query :asset (:asset tick))
                                       :filter (daterange-filter :time 
                                                                 (tcore/minus (tcore/plus (:time tick) forecast-time) focus-window-size)
                                                                 (tcore/plus (tcore/plus (:time tick) forecast-time) focus-window-size)))))]
    (- start-price end-price)))

;;; let's see how this "label" changes over a certain time period

(def ts (ticks (local-connection) 
               :query (match-query :asset "XLTCZEUR")
               :filter (daterange-filter :time 
                                         (tcore/ago (tcore/days 4))
                                         (tcore/ago (tcore/days 3)))
               :sort [{:time {:order "asc"}}]
               :size 1000))
(meta ts)
(count ts)
(map :time ts)

(label tick)


(defn )

(def t (tcore/ago (tcore/days 2)))
(def tick (first (ticks (local-connection) 
                        :query (match-query :asset "XLTCZEUR")
                        :filter (daterange-filter :time 
                                                  (tcore/ago (tcore/days 4))
                                                  (tcore/ago (tcore/days 3))))))
(:last-price tick)
;;; How do we know if the value will increase?

(def next (sort-by :time tcore/before? 
                   (all-ticks (local-connection) 
                              :query (match-query :asset (:asset tick))
                              :filter (daterange-filter :time 
                                                        (:time tick)
                                                        (tcore/plus (:time tick) (tcore/hours 3))))))
(count next) ;; 118

(map :last-price next)
(map :time next)


;;; first try: predict value in ;;; We'll try to predict 

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

;; date histogram over :last-price field of XLTCZEUR ticks:
(ticks (local-connection)
       :query (filtered-query :query (match-query :asset "XLTCZEUR")
                              :filter (daterange-filter :time (tcore/ago (tcore/hours 1))))
       :facets {:price-last-hour
                {:date_histogram {:key_field :time
                                  :value_field :last-price
                                  :interval "5m"}}}
       :search_type "count")




;;; server: 1 hour earlier
;;; 
(tformat/parse "12-11-13 14:17:51")
(pub/spread)
(def data (ticks (local-connection)
                 :query (filtered-query :query {:match {:asset "XLTCZEUR"}}
                                        :filter (daterange-filter :time (tcore/ago (tcore/hours 24))))
                 ;; :search_type "count"
                 :sort [{:time {:order "asc"}}]
                 :facets {:last-price {:date_histogram {:key_field :time
                                                        :value_field :last-price
                                                        :interval "20m"}}
                          :ask-price {:date_histogram {:key_field :time
                                                        :value_field :ask-price
                                                        :interval "20m"}}
                          :bid-price {:date_histogram {:key_field :time
                                                        :value_field :bid-price
                                                        :interval "20m"}}}
                 :size 10000))
(def ch (xy-plot (map :time data)
                 (map :last-price (:entries f))))
(view ch)
(add-lines ch (map :time (:entries a)) (map :mean (:entries a)))
(add-lines ch (map :time (:entries b)) (map :mean (:entries b)))
(set-y-range ch 17 26)



;; Volume		2.50000000
;; Stop Offset		-1.50000
;; Limit Offset		-1.55000
;; Avg. Price		€23.05000
;; Fee		€0.11525
;; Cost		€57.62500
;; Opened		12-11-13 14:17:51 +01:00
;; Closed		12-11-13 18:02:32 +01:00

(def data (ticks (local-connection)
                 :query (filtered-query :query {:match {:asset "XLTCZEUR"}}
                                        :filter (daterange-filter :time 
                                                                 (tformat/parse "2013-12-11 20:17:51")
                                                                 ;; (tformat/parse "2013-12-11 14:17:51")
                                                                 (tcore/now)
                                                                 ;; (tformat/parse "2013-12-11 18:02:32")
                                                                 ))
                 :sort [{:time {:order "asc"}}]
                 :size 10000))
(def ch (price-chart data))
(view ch)

(def ts (trades (local-connection)
                :query (filtered-query :query {:match {:asset "XLTCZEUR"}}
                                       :filter (daterange-filter :time 
                                                                 (tformat/parse "2013-12-11 20:17:51")
                                                                 ;; (tformat/parse "2013-12-11 14:17:51")
                                                                 (tcore/now)
                                                                 ;; (tformat/parse "2013-12-11 18:02:32")
                                                                 ))
                :sort [{:time {:order "asc"}}]
                :size 10000))
(add-points ch (map #(tcoerce/to-long (:time %)) ts) (map :price ts))
(first ts)
;;; note: chart times are +1
