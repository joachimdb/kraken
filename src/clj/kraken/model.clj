(ns kraken.model
  (:use [kraken.system])
  (:require [clj-time.format :as tformat]
            [clj-time.coerce :as tcoerce]
            [clj-time.core :as tcore]
            [clojure.core.async :as as]
            [taoensso.timbre :as timbre]))

(defn primary-currency [^java.lang.String market-code]
  (.substring market-code 0 (.indexOf market-code "/")))

(defn secondary-currency [^java.lang.String market-code]
  (.substring market-code (inc (.indexOf market-code "/"))))

(def mappings {:tick
               {:properties {:time {:type "date"}
                             :asset {:type "string", :index "not_analyzed"}
                             :ask-price {:type "double"}
                             :ask-volume {:type "double"}
                             :bid-price {:type "double"}
                             :bid-volume {:type "double"}
                             :last-price {:type "double"}
                             :last-volume {:type "double"}
                             :volume24 {:type "double"}
                             :trades24 {:type "integer"}
                             :low24 {:type "double"}
                             :high24 {:type "double"}
                             :opening {:type "double"}}}
               :trade 
               {:properties {:price {:type "double"}
                             :volume {:type "double"}
                             :time {:type "date"}
                             :bid-type {:type "string", :index "not_analyzed"}
                             :order-type {:type "string", :index "not_analyzed"}
                             :misc {:type "string"}}}
               :spread
               {:properties {:bid {:type "double"}
                             :ask {:type "double"}
                             :time {:type "date"}}}})

(defrecord Tick [last-trade-price last-volume volume24 high24 low24 
                 ;; spread, ask-volume, ...
                 ])
(defn mk-tick [last-trade-price last-volume volume24 high24 low24] 
  (Tick. last-trade-price last-volume volume24 high24 low24))

(defrecord Trade [^java.lang.Double price 
                  ^java.lang.Double volume
                  ^org.joda.time.DateTime time])

(defn mk-trade [price volume time]
  (Trade. price volume time))

(defrecord Order [^java.lang.Double price 
                  ^java.lang.Double volume])

(defn mk-order [price volume]
  (Order. price volume))

(defrecord OrderBook [sell-orders
                      buy-orders])

(defn mk-order-book [sell-orders buy-orders]
  ;; TO DO: orderbook itself not very interesting => compute spread/depth measures
  (OrderBook. sell-orders buy-orders))

