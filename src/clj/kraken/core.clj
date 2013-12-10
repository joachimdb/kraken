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
            [kraken.api.public :as pub]
            [clojure.core.async :as as]))


;;; See file kraken.examples.channels.clj for examples

;;; TODO: 
;;; use data in elastic to determine optimal trailing percentages for buying in and out by generating huge amount of features and doing online learning (see http://euphonious-intuition.com/2013/04/not-just-for-search-using-elasticsearch-with-machine-learning-algorithms/)

;;    "You can also use stop orders to open long or short positions. If XBT/USD price is trending up,
;; and you think a 6% fall will signal a reversal to a downtrend that you want to short, you could
;; create a trailing stop sell order with a stop offset of 6% (with leverage since it will be a
;; short position). This will open a short XBT/USD position once price falls 6%. In the reverse
;; situation, where you want to open a long position once price rises by some amount after a
;; downtrend, you could do this by creating a trailing stop buy order.""


;; (es/delete-indices (es/local-connection))
(es/create-indices (es/local-connection))
(reset! ch/+continue+ true)
(def poll-interval 5000)
;; (reset! ch/+continue+ false)

(def +pairs+ ["XLTCZEUR" "XNMCZEUR" "XXBTZEUR"])

(def es-tick-connection 
  (connect! (ch/tick-source (apply str (interpose "," +pairs+)) poll-interval false)
            (ch/tick-indexer (es/local-connection))))

(defn compute-last-spread-settings [es-connection pair]
  (let [new-spreads (pub/spread pair)
        indexed-spreads (into #{} (es/all-spreads es-connection
                                                  :query (es/match-query :asset pair)
                                                  :filter (es/daterange-filter :time (:time (first new-spreads)))))]
    ;; TODO: Use bulk api
    (doseq [s (clojure.set/intersection (into #{} new-spreads) indexed-spreads)]
      (es/index-spread es-connection s))
    {:last (:last (meta new-spreads))
     :last-processed (count (filter #(= (* 1000 (:last (meta new-spreads))) (tcoerce/to-long (:time %))) new-spreads))}))

(def es-spread-connection
  (let [es-connection (es/local-connection)]
    (doseq [pair +pairs+]
      (let [last-settings (compute-last-spread-settings es-connection pair)]
        (connect! (ch/spread-source pair poll-interval false (:last last-settings) (:last-processed last-settings))
                  (ch/spread-indexer es-connection))))))

(def es-trade-connection 
  (doseq [pair +pairs+]
    (connect! (ch/trade-source pair poll-interval false)
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
  
  
  [(get-in (es/ticks (es/local-connection) :search_type "count") [:hits :total]) ;; 3537
   (get-in (es/trades :search_type "count") [:hits :total])
   (get-in (es/spreads :search_type "count") [:hits :total])]

  (into {} (for [a ["XLTCZEUR" "XNMCZEUR" "XXBTZEUR"]]
             (vector a 
                     (select-keys (:_source (first (:hits (:hits (es/ticks :query {:match {:asset a}}
                                                                           :sort [{:time {:order "desc"}}] 
                                                                           :size 1)))))
                                  [:last-price :time]))))
  )
