(ns kraken.core
  (:use [kraken.model]
        [compojure.core]
        [ring.util.serve]) 
  (:require [clj-time.format :as tformat]
            [clj-time.coerce :as tcoerce]
            [compojure.handler :as handler]
            [compojure.route :as route]
            [kraken.elastic :as es]
            [kraken.channels :as ch]
            [clojure.core.async :as as]))


;;; See file kraken.examples.channels.clj for examples

;;; TODO: 
;;; make file and elastic sinks.
;;; Then connect ticker source to elastic sink
;;; Then do the same for spreads and trades
;;; Then save to github
;;; Then use data in elastic to determine optimal trailing percentages for buying in and out by generating huge amount of features and doing online learning (see http://euphonious-intuition.com/2013/04/not-just-for-search-using-elasticsearch-with-machine-learning-algorithms/)

;;    "You can also use stop orders to open long or short positions. If XBT/USD price is trending up,
;; and you think a 6% fall will signal a reversal to a downtrend that you want to short, you could
;; create a trailing stop sell order with a stop offset of 6% (with leverage since it will be a
;; short position). This will open a short XBT/USD position once price falls 6%. In the reverse
;; situation, where you want to open a long position once price rises by some amount after a
;; downtrend, you could do this by creating a trailing stop buy order.""


;; (es/delete-indices (es/local-connection))
(es/create-indices (es/local-connection))
(reset! ch/+continue+ true)
(def poll-interval 30000)
;; (reset! ch/+continue+ false)

(def +pairs+ ["LTCEUR" "NMCEUR" "BTCEUR"])

(def es-tick-connection 
  (connect! (ch/tick-source (apply str (butlast (interleave +pairs+ (repeat ",")))) poll-interval)
            (ch/tick-indexer (es/local-connection))))

(def es-spread-connection
  (doseq [pair +pairs+]
    (connect! (ch/spread-source pair poll-interval)
              (ch/spread-indexer (es/local-connection)))))

(def es-trade-connection 
  (doseq [pair +pairs+]
    (connect! (ch/trade-source pair poll-interval)
              (ch/trade-indexer (es/local-connection)))))


;; defroutes macro defines a function that chains individual route
;; functions together. The request map is passed to each function in
;; turn, until a non-nil response is returned.
(defroutes app-routes
  ; to serve document root address
  (GET "/" [] "<p>Hello from compojure</p>")
  ; to serve static pages saved in resources/public directory
  (route/resources "/")
  ; if page is not found
  (route/not-found "Page not found"))

;; site function creates a handler suitable for a standard website,
;; adding a bunch of standard ring middleware to app-route:
(def handler
  (handler/site app-routes))

(serve-headless handler)




(comment 
  
  
  [(get-in (es/ticks :search_type "count") [:hits :total])
   (get-in (es/trades :search_type "count") [:hits :total])
   (get-in (es/spreads :search_type "count") [:hits :total])]

  (into {} (for [a ["XLTCZEUR" "XNMCZEUR" "XXBTZEUR"]]
             (vector a 
                     (select-keys (:_source (first (:hits (:hits (es/ticks :query {:match {:asset a}}
                                                                           :sort [{:time {:order "desc"}}] 
                                                                           :size 1)))))
                                  [:last-price :time]))))
  )
