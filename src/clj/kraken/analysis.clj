(ns kraken.analysis
  (:use [kraken.model]
        [compojure.core]
        [ring.util.serve]
        [incanter.core]
        [kraken.view.charts]) 
  (:require [clj-time.format :as tformat]
            [clj-time.coerce :as tcoerce]
            [clj-time.core :as tcore]
            [kraken.elastic :as es]))



;;    "You can also use stop orders to open long or short positions. If XBT/USD price is trending up,
;; and you think a 6% fall will signal a reversal to a downtrend that you want to short, you could
;; create a trailing stop sell order with a stop offset of 6% (with leverage since it will be a
;; short position). This will open a short XBT/USD position once price falls 6%. In the reverse
;; situation, where you want to open a long position once price rises by some amount after a
;; downtrend, you could do this by creating a trailing stop buy order.""

;; Assume that we always sell when an f% fall occurs, and always buy when an r% rise
;; occurs. Depending on f and r, the outcome would be different

(def ts (es/ticks (es/local-connection)
                   :query (es/filtered-query :query {:match {:asset "XLTCZEUR"}}
                                             :filter (es/daterange-filter :time 
                                                                          (tformat/parse "2013-12-01 13:17:51")
                                                                          ;; (tformat/parse "2013-12-11 14:17:51")
                                                                          (tcore/now)
                                                                          ;; (tformat/parse "2013-12-11 18:02:32")
                                                                          ))
                   :sort [{:time {:order "asc"}}]
                   :size 10000))


;; TODO:
;; - make avging function
;; - also avg derivative
;; - compute 2nd derivative

;; tick-source --?--> suggestion source

;; ts --> dp/dt --> d2p/dt2
;;  |       |          |
;;  +-------+----------+----> suggestion source

;; OR

;; ts --> +dt/dt --> +d2p/dt2 --> suggections



(def alpha 0.99)
(def relaxation (tcore/in-millis (tcore/interval (tcore/minus (tcore/now) (tcore/days 2)) (tcore/now))))
(def nts (reduce (fn [new-ticks tick]
                   (println "start")
                   (let [prev-tick (last new-ticks)
                         dt (tcore/in-millis (tcore/interval (:time prev-tick) (:time tick)))
                         ;; the bigger dt, the more the new value counts:
                         alpha (* alpha (Math/exp (- (/ dt relaxation))))
                         new-avg-volume (+ (* alpha (:avg-bid-volume prev-tick))
                                           (* (- 1.0 alpha) (:bid-volume tick)))
                         ;; bid counts more if volume is high:
                         weighted-alpha (min 1.0 (max 0.0 (* alpha (Math/pow (/ new-avg-volume (:bid-volume tick)) 0.001))))
                         new-avg-price (+ (* weighted-alpha (:avg-bid-price prev-tick))
                                          (* (- 1.0 weighted-alpha) (:bid-price tick)))
                         db (- new-avg-price (:avg-bid-price prev-tick))
                         dv (- new-avg-volume (:avg-bid-volume prev-tick))
                         ]
                     ;; (println dt)
                     (conj new-ticks (assoc tick 
                                       :avg-bid-price new-avg-price 
                                       :dbid-price/dt (/ db dt)
                                       :avg-bid-volume new-avg-volume
                                       :dbid-volume/dt (/ dv dt)))))
                 [(assoc (first ts) 
                    :avg-bid-price (:bid-price (first ts))
                    :avg-bid-volume (:bid-volume (first ts)))]
                 (rest ts)))
(first nts)
(def ch (price-chart nts :bid-price :avg-bid-price))
(view ch)

(def ch (price-chart nts :avg-bid-volume :bid-volume))
(view ch)

(def ch (price-chart nts :dbid-price/dt))
(view ch)


(def times (map :time ts))
(def dt (map #(tcore/in-seconds (tcore/interval %1 %2)) (butlast times) (drop 1 times)))
(def bids (map :bid-price ts))
(def dbids (map - (butlast bids) (drop 1 bids)))
(def Dbids (map / dbids dt))
(def dDbids (map - (butlast bids) (drop 1 bids)))
(count Dbids)
(def sell-stop 0.05)
(def buy-stop 0.05)
(def monitoring interval)



(defn sell? [current high]
  (<= current (* high (- 1 sell-stop))))
(defn buy? [current low]
  (>= current (* low (- 1 buy-stop))))

(defn process-sell-order [tick order]
  (if (sell? (:bid tick) (:high order))
    {:order nil :funds (* (:bid tick) (:amount order))}
    {:order (update-in order [:high] #(max % (:high tick)))}))

(defn process-buy-order [tick order]
  (if (buy? (:ask tick) (:low order))
    {:order nil :funds (- (* (:ask tick) (:amount order)))}
    {:order (update-in order [:low] #(min % (:low tick)))}))

(def simulate [ticks bank orders]
  (let [current-ask (:ask tick)
        current-bid (:bid tick)]

    ())

  )


