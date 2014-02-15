(ns kraken.api.kraken
  (:use [kraken.model]
        [kraken.channels])
  (:require [clojure.core.async :as as]
            [clj-time.core :as tcore]
            [clj-time.coerce :as tcoerce]
            [clj-http.client :as http]
            [clojure.data.json :as json]))

;;; DEFINITIONS
;;; ===========

(def api-domain (atom "https://api.kraken.com/"))

(defn api-url [x & rest] (apply str @api-domain x rest))

(defn param-string [key-map]
  (apply str (interpose "&"
                        (map #(str (name (key %)) "=" (val %)) key-map))))

(defn url-with-params [url key-map]
  (str url "?" (param-string key-map)))

(defn asset->market-code [asset]
  (.substring (name asset) 1))

(def market-codes->pairs {"EUR/XVN" "ZEURXXVN" 
                          "NMC/EUR" "XNMCZEUR" 
                          "LTC/USD" "XLTCZUSD" 
                          "NMC/KRW" "XNMCZKRW" 
                          "EUR/XRP" "ZEURXXRP" 
                          "BTC/XRP" "XXBTXXRP" 
                          "BTC/NMC" "XXBTXNMC" 
                          "USD/XRP" "ZUSDXXRP" 
                          "BTC/XVN" "XXBTXXVN" 
                          "BTC/LTC" "XXBTXLTC" 
                          "BTC/KRW" "XXBTZKRW" 
                          "BTC/EUR" "XXBTZEUR" 
                          "NMC/USD" "XNMCZUSD" 
                          "LTC/EUR" "XLTCZEUR" 
                          "BTC/USD" "XXBTZUSD" 
                          "USD/XVN" "ZUSDXXVN" 
                          "LTC/XRP" "XLTCXXRP" 
                          "XVN/XRP" "XXVNXXRP" 
                          "NMC/XRP" "XNMCXXRP" 
                          "KRW/XRP" "ZKRWXXRP" 
                          "LTC/KRW" "XLTCZKRW"})

(def pairs->market-codes (zipmap (vals market-codes->pairs) (keys market-codes->pairs)))

(defn- public-url [x & rest] 
  (apply api-url "0/public/" x rest))

;;; PUBLIC API
;;; ==========

(defn server-time []
  "URL: https://api.kraken.com/0/public/Time

   Result: Server's time
   unixtime =  as unix timestamp
   rfc1123 = as RFC 1123 time format

   Note: This is to aid in approximating the skew time between the server and client.
"
  (let [response (http/get (public-url "Time"))
        body (json/read-json (:body response))]
    (:result body)))

(defn assets [& {:keys [info aclass asset] :or {info "info" aclass "currency"} :as opts}]
  "URL: https://api.kraken.com/0/public/Assets

   Input:
   info = info to retrieve (optional):
       info = all info (default)
   aclass = asset class (optional):
       currency (default)
   asset = comma delimited list of assets to get info on (optional.  default = all for given asset class)

   Result: array of asset names and their info
   <asset_name> = asset name
       altname = alternate name
       aclass = asset class
       decimals = scaling decimal places for record keeping
       display_decimals = scaling decimal places for output display
"
  (let [response (http/get (url-with-params (public-url "Assets") opts))
        body (json/read-json (:body response))]
    (zipmap (map asset->market-code (keys (:result body)))
            (vals (:result body)))))
;; (assets :asset "LTC")
;; (assets)

(defn markets [& {:keys [info market-codes] 
                  :or {info "info" market-codes "all"}
                  :as opts}]
  "URL: https://api.kraken.com/0/public/AssetPairs

   Input:
   info = info to retrieve (optional):
       info = all info (default)
       leverage = leverage info
       fees = fees schedule
       margin = margin info
   pair = comma delimited list of asset pairs to get info on (optional.  default = all)
 
   Result: array of pair names and their info
   <pair_name> = pair name
       altname = alternate pair name
       aclass_base = asset class of base component
       base = asset id of base component
       aclass_quote = asset class of quote component
       quote = asset id of quote component
       lot = volume lot size
       pair_decimals = scaling decimal places for pair
       lot_decimals = scaling decimal places for volume
       lot_multiplier = amount to multiply lot volume by to get currency volume
       leverage = array of leverage amounts available
       fees = fee schedule array in [volume, percent fee] tuples
       fee_volume_currency = volume discount currency
       margin_call = margin call level
       margin_stop = stop-out/liquidation margin level
"
  (let [pairs (if (= "all" market-codes) 
                ""
                (apply str "&pair=" (interpose "," (map market-codes->pairs market-codes))))
        _ (println pairs)
        response (http/get (url-with-params (public-url "AssetPairs?" pairs) {}))
        body (json/read-json (:body response))]
    (zipmap (map #(pairs->market-codes (name %)) (keys (:result body)))
            (vals (:result body)))))


(defn- parse-tick [tick]
  {:ask-price (double (read-string (first (get-in tick [:a]))))
   :ask-volume (double (read-string (second (get-in tick [:a]))))
   :bid-price (double (read-string (first (get-in tick [:b]))))
   :bid-volume (double (read-string (second (get-in tick [:b]))))
   :last-price (double (read-string (first (get-in tick [:c]))))
   :last-volume (double (read-string (second (get-in tick [:c]))))
   :volume24 (double (read-string (first (get-in tick [:v]))))
   :trades24 (long (first (get-in tick [:t])))
   :low24 (double (read-string (first (get-in tick [:l]))))
   :high24 (double (read-string (first (get-in tick [:h]))))
   :opening (double (read-string (get-in tick [:o])))})

(defn- ticks [& market-codes]
  "URL: https://api.kraken.com/0/public/Ticker

   Input:
   markets = market codes, e.g. \"DOGE/BTC\"

   Result: map of market id to ticks info (see parse-tick)

   Note: Today's prices start at 00:00:00 UTC
"
  (zipmap market-codes 
          (map (fn [pair]
                 (let [response (http/get (url-with-params (public-url "Ticker") {:pair pair}))
                       body (json/read-json (:body response))
                       ctime (tcore/now)]
                   (when (empty? (get-in body [:error]))
                     (for [[symb tick-data] (:result body)]
                       (parse-tick tick-data)))))
               (map market-codes->pairs market-codes))))

(defn tick-channel 
  "Polling channel sending ticks."
  [market-code & {:keys [interval]
                  :or {interval (* 1000 60)}}]
  (as/mapcat< (fn [ticks]
                (map (fn [tick]
                       (apply mk-tick ((juxt :last-price :last-volume :volume24 :high24 :low24) tick))))
                ticks)
              (polling-channel #(get (ticks market-code) market-code)
                               :interval interval 
                               :on-fail [])))

(defn- parse-spread [[time bid ask]]
  {:bid (double (read-string bid))
   :ask (double (read-string ask))
   :time (tcoerce/from-long (* 1000 time))})

(defn- spread 
  "URL: https://api.kraken.com/0/public/Spread

   Input:
   pair = asset pair to get spread data for
   since = return spread data since given id (inclusive)

   Result: array of pair name and recent spread data
   <pair_name> = pair name
       array of array entries(<time>, <bid>, <ask>)
   last = id to be used as since when polling for new spread data

   Note: \"since\" is inclusive so any returned data with the same time as the previous set should overwrite all of the previous set's entries at that time"
  [market-code & {:keys [since] :as opts}]
  (println "calling spread, since = " since)
  (let [pair (market-codes->pairs market-code)
        response (http/get (url-with-params (public-url "Spread") (assoc opts :pair pair)))
        body (json/read-json (:body response))]
    (when-not (empty? (:error body))
      (throw (Exception. (str (:error body)))))
    (with-meta (mapcat (fn [[pair data]]
                         (map parse-spread data))
                       (dissoc (:result body) :last))
      {:last (get-in body [:result :last])
       :market-code market-code})))
;; (spread "LTC/EUR")

(defn spread-channel 
  "Polling channel sending spread information"
  [market-code & {:keys [interval since]
                  :or {interval (* 1000 60)}}]
  (let [ch (as/chan)
        last (atom nil)
        last-processed (atom 0)
        get-spread (fn [] (try (spread market-code :since @last)
                               (catch Exception e false)))]
    (as/go 
      (loop []
        (when-let [next (get-spread)]
          (when (> (count next) @last-processed)
            (as/>! ch (drop @last-processed next)))
          (reset! last (:last (meta next)))
          (reset! last-processed (count (filter #(= (* 1000 @last) (tcoerce/to-long (:time %))) next))))
        (as/<! (as/timeout interval))
        (recur)))
    (as/mapcat< identity ch)))

;; (defn- pair-code [market-code]
;;   (clojure.string/replace (name market-code) #"[ZX](...)[ZX](...)" "$1/$2"))

;; TODO: 
;; take care of exceptions
;; - do same change to spread/orders as done to ticker (i.e. parse-spread, ...)
;; - use principles here to make cryptsy channels
;; - Look at core to connect channels to elastic

(defn- parse-depth-entry [[price volume time]]
  {:price (read-string price) 
   :volume (read-string volume) 
   :time (tcoerce/from-long (* 1000 time))})
  

(defn- depth [market-code & {:keys [count] :as opts}]
  "URL: https://api.kraken.com/0/public/Depth

   Input:
   pair = asset pair to get market depth for
   count = maximum number of asks/bids (optional)

   Result: array of pair name and market depth
   <pair_name> = pair name
       asks = ask side array of array entries(<price>, <volume>, <timestamp>)
       bids = bid side array of array entries(<price>, <volume>, <timestamp>)
"
  (let [pair (market-codes->pairs market-code)
        response (http/get (url-with-params (public-url "Depth") (assoc opts :pair pair)))
        body (json/read-json (:body response))]
    (when-not (empty? (:error body))
      (throw (Exception. (str (:error body)))))
    (zipmap (keys (val (first (:result body))))
            (map #(mapv parse-depth-entry %)  (vals (val (first (:result body))))))))

(defn- order-book [market-code]
  (let [depth-data (depth market-code)]
    (mk-order-book market-code "kraken"
                   (map #(mk-order (:price %) (:volume %))
                        (:asks depth-data))
                   (map #(mk-order (:price %) (:volume %))
                        (:bids depth-data)))))

;; (depth "LTC/BTC")
(defn order-book-channel [market-code & {:keys [interval]
                                         :or {interval (* 1000 60)}}]
  (as/filter< identity ;; filter out on-fails (false)
              (polling-channel #(order-book market-code)
                               :interval interval
                               :on-fail false)))

;;;;;; REMAINING CODE STILL NEEDS TO BE REFACTORED

(defn- parse-trade [[price volume time bid-type order-type misc]]  
  {:price (double (read-string price))
   :volume (double (read-string price))
   :time (tcoerce/from-long (* 1000 (long time)))
   :bid-type bid-type
   :order-type order-type
   :misc misc})

(defn trades [market-code & {:keys [since] :as opts}]
  "URL: https://api.kraken.com/0/public/Trades

   Input:
   pair = asset pair to get trade data for
   since = return trade data since given id (exclusive)

   Result: 
      array entries(<price>, <volume>, <time>, <buy/sell>, <market/limit>, <miscellaneous>)

   Metadata:
      :last = id to be used as since when polling for new trade data
"
  (let [pair (market-codes->pairs market-code)
        response (http/get (url-with-params (public-url "Trades") (assoc opts :pair pair)))
        body (json/read-json (:body response))]
    (when-not (empty? (:error body))
      (throw (Exception. (str (:error body)))))
    (with-meta (mapv parse-trade (get-in body [:result (keyword pair)]))
      {:last (get-in body [:result :last])})))
;; (def ts (trades "LTC/EUR" ))
;; (meta ts)

(defn trade-channel [market-code & {:keys [interval]
                                    :or {interval (* 1000 60)}}]
  (as/mapcat< (fn [trades]
                (map (fn [trade]
                       (apply mk-trade ((juxt :price :volume :time) trade)))
                     trades)) ;; trades returns a list of trades
              (recursive-channel (fn [prev] (trades market-code :since (:last (meta prev))))
                                 nil
                                 :interval interval
                                 :on-fail false)))


;; (def tc (trade-channel "LTC/EUR" :interval 5000))
;; (dotimes [i 100]
;;   (as/take! tc (fn [v] (println "Got " v) (flush))))
;; (as/close! tc)


