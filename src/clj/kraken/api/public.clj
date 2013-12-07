(ns kraken.api.public
  (:use [kraken.api.core]
        [kraken.model])
  (:require [clj-time.core :as tcore]
            [clj-time.coerce :as tcoerce]
            [clj-http.client :as http]
            [clojure.data.json :as json]))

;;; TODO: handle bad responses and timeouts

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
    (:result body)))


(defn asset-pairs [& {:keys [info pair] :as opts}]
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
  (let [response (http/get (url-with-params (public-url "AssetPairs") opts))
        body (json/read-json (:body response))]
    (:result body)))


(defn ticker [pair]
  "URL: https://api.kraken.com/0/public/Ticker

   Input:
   pair = comma delimited list of asset pairs to get info on

   Result: array of pair names and their ticker info
   <pair_name> = pair name
       a = ask array(<price>, <lot volume>),
       b = bid array(<price>, <lot volume>),
       c = last trade closed array(<price>, <lot volume>),
       v = volume array(<today>, <last 24 hours>),
       p = volume weighted average price array(<today>, <last 24 hours>),
       t = number of trades array(<today>, <last 24 hours>),
       l = low array(<today>, <last 24 hours>),
       h = high array(<today>, <last 24 hours>),
       o = today's opening price
   Note: Today's prices start at 00:00:00 UTC
"
  (let [response (http/get (url-with-params (public-url "Ticker") {:pair pair}))
        body (json/read-json (:body response))
        ctime (tcore/now)]
    (when (empty? (get-in body [:error]))
      (for [[symb tick-data] (:result body)]
        (mk-tick ctime
                 (name symb)
                 (double (read-string (first (get-in tick-data [:a]))))
                 (double (read-string (second (get-in tick-data [:a]))))
                 (double (read-string (first (get-in tick-data [:b]))))
                 (double (read-string (second (get-in tick-data [:b]))))
                 (double (read-string (first (get-in tick-data [:c]))))
                 (double (read-string (second (get-in tick-data [:c]))))
                 (double (read-string (first (get-in tick-data [:v]))))
                 (long (first (get-in tick-data [:t])))
                 (double (read-string (first (get-in tick-data [:l]))))
                 (double (read-string (first (get-in tick-data [:h]))))
                 (double (read-string (get-in tick-data [:o]))))))))
           

(defn parse-depth-entry [[price volume time]]
  {:price (read-string price) 
   :volume (read-string volume) 
   :time time})
(defn depth [pair & {:keys [count] :as opts}]
  "URL: https://api.kraken.com/0/public/Depth

   Input:
   pair = asset pair to get market depth for
   count = maximum number of asks/bids (optional)

   Result: array of pair name and market depth
   <pair_name> = pair name
       asks = ask side array of array entries(<price>, <volume>, <timestamp>)
       bids = bid side array of array entries(<price>, <volume>, <timestamp>)
"
  (let [response (http/get (url-with-params (public-url "Depth") (assoc opts :pair pair)))
        body (json/read-json (:body response))]
    (zipmap (keys (:result body))
            (map (fn [m]
                   (zipmap (keys m)
                           (mapv #(map parse-depth-entry %) (vals m))))
                 (vals (:result body))))))

(defn trades [pair & {:keys [since] :as opts}]
  "URL: https://api.kraken.com/0/public/Trades

   Input:
   pair = asset pair to get trade data for
   since = return trade data since given id (exclusive)

   Result: 
      array entries(<price>, <volume>, <time>, <buy/sell>, <market/limit>, <miscellaneous>)

   Metadata:
      :last = id to be used as since when polling for new trade data
"
  (let [response (http/get (url-with-params (public-url "Trades") (assoc opts :pair pair)))
        body (json/read-json (:body response))]
    (when-not (empty? (:error body))
      (throw (Exception. (str (:error body)))))
    (with-meta (map #(mk-trade (double (read-string (nth % 0)))
                               (double (read-string (nth % 1)))
                               (tcoerce/from-long (* 1000 (long (nth % 2))))
                               (nth % 3)
                               (nth % 4)
                               (nth % 5)
                               (name (key (first (dissoc (:result body) :last)))))
                    (val (first (dissoc (:result body) :last))))
      {:last (get-in body [:result :last])})))

(defn spread [pair & {:keys [since] :as opts}]
  "URL: https://api.kraken.com/0/public/Spread

   Input:
   pair = asset pair to get spread data for
   since = return spread data since given id (inclusive)

   Result: array of pair name and recent spread data
   <pair_name> = pair name
       array of array entries(<time>, <bid>, <ask>)
   last = id to be used as since when polling for new spread data

   Note: \"since\" is inclusive so any returned data with the same time as the previous set should overwrite all of the previous set's entries at that time"
  (let [response (http/get (url-with-params (public-url "Spread") (assoc opts :pair pair)))
        body (json/read-json (:body response))]
    (with-meta (mapcat (fn [[pair data]]
                         (map (fn [e]
                                (mk-spread (double (read-string (nth e 1)))
                                           (double (read-string (nth e 2)))
                                           (tcoerce/from-long (* 1000 (nth e 0)))
                                           (name pair)))
                              data))
                       (dissoc (:result body) :last))
      {:last (get-in body [:result :last])})))
    


;;; (def s (spread "LTCEUR"))
;;; (def s (spread "LTCEUR,BTCEUR"))
;;; (def t (trades "LTCEUR"))
;;; (def s (trades "LTCEUR,BTCEUR"))
;;; (meta s)
;;; (first s)
;;; (def t (trades "LTCEUR"))
;;; (def r (http/get (url-with-params (public-url "Trades") {:pair "LTCEUR,BTCEUR"})))
