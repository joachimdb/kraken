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

(defprotocol DocumentP
  (index [this system])
  (mapping-type [this system])
  (document [this system]))

(defn index-name [system exchange-code]
  (str "trades-" exchange-code))

(defrecord Trade [^java.lang.Double price 
                  ^java.lang.Double volume
                  ^org.joda.time.DateTime time
                  ^java.lang.String market-code
                  ^java.lang.String exchange-code]
  DocumentP
  (index [this system] (index-name system exchange-code))
  (mapping-type [this system] :trade)
  (document [this system]
    (select-keys this [:market-code :price :volume :time])))

(defn mk-trade [exchange-code market-code price volume time]
  (Trade. price volume time market-code exchange-code))


