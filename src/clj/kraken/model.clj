(ns kraken.model
  (:require [clj-time.format :as tformat]
            [clj-time.coerce :as tcoerce]
            [clojure.core.async :as as]))

(def currencies ["LTC" "BTC" "DOGE"])

(defn primary-currency [^java.lang.String market-code]
  (.substring market-code 0 (.indexOf market-code "/")))

(defn secondary-currency [^java.lang.String market-code]
  (.substring market-code (inc (.indexOf market-code "/"))))

(def mappings {:trade 
               {:properties {:price {:type "double"}
                             :volume {:type "double"}
                             :time {:type "date"}
                             :bid-type {:type "string", :index "not_analyzed"}
                             :order-type {:type "string", :index "not_analyzed"}
                             :misc {:type "string"}
                             :market {:type "string", :index "not_analyzed"}
                             :exchange {:type "string", :index "not_analyzed"}}}
               :order 
               {:properties {:volume {:type "double"}
                             :price {:type "double"}
                             :bid-type {:type "string", :index "not_analyzed"}
                             :order-type {:type "string", :index "not_analyzed"}
                             :market {:type "string", :index "not_analyzed"}
                             :exchange {:type "string", :index "not_analyzed"}}}})

(defrecord Trade [^java.lang.Double price 
                  ^java.lang.Double volume
                  ^org.joda.time.DateTime time
                  ^java.lang.String bid-type
                  ^java.lang.String order-type
                  ^java.lang.String misc
                  ^java.lang.String market
                  ^java.lang.String exchange])

(defn mk-trade [price volume time bid-type order-type misc market exchange]
  (Trade. price volume time bid-type order-type misc market exchange))

(defrecord Order [^java.lang.Double price 
                  ^java.lang.Double volume])

(defn mk-order [price volume]
  (Order. price volume))

(defrecord OrderBook [^java.lang.String market-code
                      ^java.lang.String exchange-code
                      sell-orders
                      buy-orders])

(defn mk-order-book [market-code exchange-code sell-orders buy-orders]
  (OrderBook. market-code exchange-code sell-orders buy-orders))

;; orderbook itself not very interesting. 

(def obk (kraken.api.kraken/order-book "BTC/LTC"))
(def obc (kraken.api.cryptsy/order-book "LTC/BTC"))

obk
{:market-code "BTC/LTC",
 :exchange-code "kraken",
 :sell-orders
 ({:price 38.8, :volume 0.301}
  {:price 38.9, :volume 2.718}
  {:price 38.985, :volume 0.204}),
 :buy-orders
 ({:price 38.3, :volume 0.028}
  {:price 38.131, :volume 0.019}
  {:price 37.942, :volume 0.019})}

obc

{:market-code "LTC/BTC",
 :exchange-code "kraken",
 :sell-orders
 ({:price 0.02599988, :volume 25.69824808}
  {:price 0.0259999, :volume 104.83520566}
  {:price 0.02599998, :volume 47.71399946}),
 :buy-orders
 ({:price 0.0259, :volume nil}
  {:price 0.02587007, :volume nil}
  {:price 0.02587006, :volume nil}
  {:price 0.02587003, :volume nil})}

;;; sell-orders is 
