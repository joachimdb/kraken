(ns kraken.model
  (:use [kraken.system]
        [kraken.elastic])
  (:require [clj-time.format :as tformat]
            [clj-time.coerce :as tcoerce]
            [clj-time.core :as tcore]
            [clojure.core.async :as as]
            [taoensso.timbre :as timbre]))

(defn primary-currency [^java.lang.String market-code]
  (.substring market-code 0 (.indexOf market-code "/")))

(defn secondary-currency [^java.lang.String market-code]
  (.substring market-code (inc (.indexOf market-code "/"))))

(defn index-name [system exchange-code]
  (str "trades-" exchange-code))

(defrecord Trade [^java.lang.Double price 
                  ^java.lang.Double volume
                  ^org.joda.time.DateTime time
                  ^java.lang.String market-code
                  ^java.lang.String exchange-code
                  ^java.lang.String id]
  DocumentP
  (index [this system] (index-name system exchange-code))
  (mapping-type [this system] "trade")
  (document [this system]
    (select-keys this [:market-code :price :volume :time :id])))

(defn mk-trade [exchange-code market-code price volume time id]
  (Trade. price volume time market-code exchange-code id))


(defn trades [system 
              & {:keys [exchange-code market-code filter size search_type sort fields]
                 :or {exchange-code "cryptsy" market-code "DOGE/BTC" sort {:time {:order "desc"}}}
                 :as opts}]
  (let [hits (apply hits system (index-name system exchange-code) "trade"
                    :query {:term {:market-code market-code}}
                    :sort sort
                    (flatten (seq (dissoc opts [:exchange-code :market-code]))))]
    (with-meta (map #(with-meta (mk-trade exchange-code 
                                          (:market-code %)
                                          (:price %)
                                          (:volume %)
                                          (tcoerce/from-long (:time %))
                                          (:id %))
                       (meta %))
                    hits)
      (meta hits))))

(defn price-curve [system interval
                   & {:keys [exchange-code market-code filter time_zone factor pre_offset post_offset]
                      :or {exchange-code "cryptsy" market-code "DOGE/BTC"}
                      :as opts}]
  (apply date-histogram system (index-name system exchange-code) "trade" :time :price interval
         :query {:term {:market-code market-code}}
         (flatten (seq (dissoc opts [:exchange-code :market-code])))))

(defn volume-curve [system interval
                    & {:keys [exchange-code market-code filter time_zone factor pre_offset post_offset]
                       :or {exchange-code "cryptsy" market-code "DOGE/BTC"}
                       :as opts}]
  (apply date-histogram system (index-name system exchange-code) "trade" :time :volume interval
         :query {:term {:market-code market-code}}
         (flatten (seq (dissoc opts [:exchange-code :market-code])))))

