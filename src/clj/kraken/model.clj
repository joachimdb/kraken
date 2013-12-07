(ns kraken.model
  (:require [clj-time.format :as tformat]
            [clj-time.coerce :as tcoerce]
            [clojure.core.async :as as]))

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

(defrecord Tick [^org.joda.time.DateTime time 
                 ^java.lang.String asset
                 ^java.lang.Double ask-price
                 ^java.lang.Double ask-volume 
                 ^java.lang.Double bid-price 
                 ^java.lang.Double bid-volume 
                 ^java.lang.Double last-price 
                 ^java.lang.Double last-volume 
                 ^java.lang.Double volume24 
                 ^java.lang.Long trades24
                 ^java.lang.Double low24
                 ^java.lang.Double high24
                 ^java.lang.Double opening])

(defn mk-tick [time asset ask-price ask-volume bid-price bid-volume last-price last-volume volume24 trades24 low24 high24 opening]
  (Tick. time asset ask-price ask-volume bid-price bid-volume last-price last-volume volume24 trades24 low24 high24 opening))


(defrecord Trade [^java.lang.Double price 
                  ^java.lang.Double volume
                  ^org.joda.time.DateTime time
                  ^java.lang.String bid-type
                  ^java.lang.String order-type
                  ^java.lang.String misc
                  ^java.lang.String asset])

(defn mk-trade [price volume time bid-type order-type misc asset]
  (Trade. price volume time bid-type order-type misc asset))


(defrecord Spread [^java.lang.Double bid 
                   ^java.lang.Double ask 
                   ^org.joda.time.DateTime time
                   ^java.lang.String asset])

(defn mk-spread [bid ask time asset]
  (Spread. bid ask time asset))

(defprotocol ConnectionP
  (disconnect! [this]))

(defrecord Connection [source sink channel]
  ConnectionP
  (disconnect! [this] 
    (as/untap source channel)
    (as/unmix sink channel)
    (as/close! channel)
    nil))

(defn connect! 
  "Creates and returns a channel connecting a source and a sink."
  ([source sink] (connect! source sink (as/chan)))
  ([source sink chan]
     (as/tap source chan)
     (as/admix sink chan)
     (Connection. source sink chan)))
