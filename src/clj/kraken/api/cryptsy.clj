(ns kraken.api.cryptsy
  (:use [kraken.model]
        [kraken.channels])
  (:require [clojure.core.async :as as]
            [clj-time.core :as tcore]
            [clj-time.coerce :as tcoerce]
            [clj-time.format :as tformat]
            [clj-http.client :as http]
            [clojure.data.json :as json]))

(defn cryptsy-public [method &{:keys [query-params] 
                               :or {query-params {}}}] 
  (json/read-str (:body (http/get (str "http://pubapi.cryptsy.com/api.php?method=" method)
                                  {:query-params query-params} ))))

(defn- parse-trade [trade]
  {:price (read-string (get trade "price"))
   :quantity (read-string (get trade "quantity"))
   :time (clj-time.format/parse (get trade "time"))
   :id (read-string (get trade "id"))
   :total (read-string (get trade "total"))})

(defn- parse-order [order]
  {:price (read-string (get order "price"))
   :quantity (read-string (get order "quantity"))
   :total (read-string (get order "total"))})

(defn- parse-market-data [md]
  {:market-id (read-string (get md "marketid"))
   :label (get md "label")
   :primary-name (get md "primaryname")
   :primary-code (get md "primarycode")
   :secondary-name (get md "secondaryname")
   :secondary-code (get md "secondarycode")
   :last-trade-price (when (contains? md "lasttradeprice")
                       (read-string (get md "lasttradeprice")))
   :last-trade-time (tformat/parse (get md "lasttradetime"))
   :volume (when (contains? md "volume")
             (read-string (get md "volume")))
   :recent-trades (mapv parse-trade (get md "recenttrades"))
   :sell-orders (mapv parse-order (get md "sellorders"))
   :buy-orders (mapv parse-order (get md "buyorders"))})

(defn compute-market-codes->ids []
  (let [md (get-in (cryptsy-public "marketdatav2") ["return" "markets"])]
    (zipmap (keys md)
            (map #(read-string (get % "marketid")) (vals md)))))

;; (def market-codes->ids (compute-market-codes->ids))
(def market-codes->ids {"LEAF/BTC" 148,
                        "GLC/BTC" 76,
                        "CGB/BTC" 70,
                        "SBC/LTC" 128,
                        "42/BTC" 141,
                        "NRB/BTC" 54,
                        "GLD/BTC" 30,
                        "MEM/LTC" 56,
                        "FRK/BTC" 33,
                        "ORB/BTC" 75,
                        "OSC/BTC" 144,
                        "XPM/LTC" 106,
                        "MEOW/BTC" 149,
                        "WDC/LTC" 21,
                        "DGC/BTC" 26,
                        "ANC/LTC" 121,
                        "NET/XPM" 104,
                        "MOON/BTC" 146,
                        "TIPS/LTC" 147,
                        "MNC/BTC" 7,
                        "CNC/LTC" 17,
                        "PPC/BTC" 28,
                        "FFC/BTC" 138,
                        "Points/BTC" 120,
                        "NMC/BTC" 29,
                        "SRC/BTC" 88,
                        "VTC/BTC" 151,
                        "TRC/BTC" 27,
                        "PXC/LTC" 101,
                        "RPC/BTC" 143,
                        "RYC/LTC" 37,
                        "GDC/BTC" 82,
                        "NET/LTC" 108,
                        "TIX/XPM" 103,
                        "FST/BTC" 44,
                        "KGC/BTC" 65,
                        "EAC/BTC" 139,
                        "SXC/LTC" 98,
                        "CGB/LTC" 123,
                        "CLR/BTC" 95,
                        "GLD/LTC" 36,
                        "ZET/BTC" 85,
                        "GME/LTC" 84,
                        "DOGE/BTC" 132,
                        "QRK/BTC" 71,
                        "DGC/LTC" 96,
                        "IFC/XPM" 105,
                        "MOON/LTC" 145,
                        "MEC/BTC" 45,
                        "DEM/BTC" 131,
                        "NEC/BTC" 90,
                        "JKC/LTC" 35,
                        "CENT/XPM" 118,
                        "TIX/LTC" 107,
                        "PPC/LTC" 125,
                        "CPR/LTC" 91,
                        "PTS/BTC" 119,
                        "IFC/LTC" 60,
                        "BTB/BTC" 23,
                        "TGC/BTC" 130,
                        "DVC/BTC" 40,
                        "STR/BTC" 83,
                        "LOT/BTC" 137,
                        "FST/LTC" 124,
                        "BET/BTC" 129,
                        "CAP/BTC" 53,
                        "BTE/BTC" 49,
                        "FLO/LTC" 61,
                        "ELP/LTC" 93,
                        "CASH/BTC" 150,
                        "GLX/BTC" 78,
                        "ZET/LTC" 127,
                        "DOGE/LTC" 135,
                        "CSC/BTC" 68,
                        "HBN/BTC" 80,
                        "SBC/BTC" 51,
                        "QRK/LTC" 126,
                        "IXC/BTC" 38,
                        "CRC/BTC" 58,
                        "BQC/BTC" 10,
                        "MEC/LTC" 100,
                        "XPM/BTC" 63,
                        "FTC/BTC" 5,
                        "DVC/XPM" 122,
                        "BTG/BTC" 50,
                        "ASC/XPM" 112,
                        "CAT/BTC" 136,
                        "UNO/BTC" 133,
                        "ANC/BTC" 66,
                        "ARG/BTC" 48,
                        "WDC/BTC" 14,
                        "BCX/BTC" 142,
                        "FRC/BTC" 39,
                        "AMC/BTC" 43,
                        "SPT/BTC" 81,
                        "XNC/LTC" 67,
                        "EZC/LTC" 55,
                        "NBL/BTC" 32,
                        "CNC/BTC" 8,
                        "MST/LTC" 62,
                        "DBL/LTC" 46,
                        "BUK/BTC" 102,
                        "LK7/BTC" 116,
                        "CMC/BTC" 74,
                        "YBC/BTC" 73,
                        "LKY/BTC" 34,
                        "PYC/BTC" 92,
                        "TAG/BTC" 117,
                        "TEK/BTC" 114,
                        "RED/LTC" 87,
                        "PXC/BTC" 31,
                        "NVC/BTC" 13,
                        "LTC/BTC" 3,
                        "DVC/LTC" 52,
                        "DMD/BTC" 72,
                        "ASC/LTC" 111,
                        "NAN/BTC" 64,
                        "PHS/BTC" 86,
                        "ALF/BTC" 57,
                        "ELC/BTC" 12,
                        "EMD/BTC" 69,
                        "XJO/BTC" 115,
                        "NET/BTC" 134})

(defn market-data 
 "General Market Data

  Returns a map of market data maps

  Example: 

  (def md (market-data \"DOGE/BTC\" \"DOGE/LTC\"))

  (keys md) => (\"DOGE/LTC\" \"DOGE/BTC\") 
  
  (get md \"DOGE/LTC\")
  => {:market-id 132,
      :label :DOGE/BTC,
      :primaryname \"Dogecoin\",
      :primarycode :DOGE,
      :secondaryname \"BitCoin\",
      :secondarycode :BTC,
      :lasttradeprice 0.00000219
      :lasttradetime #<DateTime 2014-01-25T17:20:23.000Z>,
      :volume \"1765529079.68198780\"
      :recent-trades
      [{:price 1.58E-6, :quantity 23161.61437057, :time #<DateTime 2014-02-05T17:29:13.000Z>, :id 21585792, :total 0.03659535}
       {:price 1.59E-6, :quantity 14502.96282435, :time #<DateTime 2014-02-05T17:29:06.000Z>, :id 21585788, :total 0.02305971}
    ...],       
      :sell-orders
      [{:price 0.00000218, :quantity 148613.86735303, :total 2.18000000} 
       {:price 0.00000219, :quantity 474041.74876534, :total 1.03815143}
       ...],
      :buy-orders
      [{:price 0.00000218, :quantity 0.00000000, :total 0.02180000}
       {:price 0.00000217, :quantity 1902765.84653952, :total 4.61028438}
       {:price 0.00000216, :quantity 22174803.88215621, :total 47.89757637}
       ...]}
"
  [& market-labels]
  (if (empty? market-labels)
    (let [all-mdata (try (get-in (cryptsy-public "marketdatav2") ["return" "markets"])
                         (catch Exception e {}))]
      (zipmap (keys all-mdata)
              (map parse-market-data (vals all-mdata))))
    (zipmap market-labels
            (map #(try (parse-market-data 
                        (val 
                         (first 
                          (get-in (cryptsy-public "singlemarketdata" :query-params {:marketid %})
                                  ["return" "markets"]))))
                       (catch Exception e {}))
                 (map market-codes->ids market-labels)))))
                 
;; (reset-market-ids!)


(defn orders 
  "Same as market-data, but doesn't return values for any trade related keys"
  [& market-codes]
  (if (empty? market-codes)
    (let [all-orders (try (get (cryptsy-public "orderdata") "return")
                          (catch Exception e {}))]
      (zipmap (keys all-orders)
              (map parse-market-data (vals all-orders))))
    (zipmap market-codes
            (map (fn [market-code]
                   (if-let [md (try (val (first (get (cryptsy-public "singleorderdata" :query-params {:marketid (market-codes->ids market-code)})
                                                     "return")))
                                    (catch Exception e false))]
                     (dissoc (parse-market-data md)
                             :recent-trades :last-trade-time :last-trade-price :volume)
                     {}))
                 market-codes))))
;; (orders "LTC/BTC")

(defn order-book [market-code]
  (let [orders (get (orders market-code) market-code)]
    (mk-order-book market-code "kraken"
                   (map #(mk-order (:price %) (:quantity %))
                        (:sell-orders orders))
                   (map #(mk-order (:price %) (:volume %))
                        (:buy-orders orders)))))

(defn order-book-channel [market-code & {:keys [interval]
                                         :or {interval (* 1000 60)}}]
  (as/filter< identity ;; filter out on-fails (false)
              (polling-channel #(order-book market-code)
                               :interval interval
                               :on-fail false)))



;; ;;; private api

;; (def public-key "9ce5ef061247770f79a6f6213b4e7c31cf655c3f")
;; (def private-key (slurp "/Users/joachim/.cryptsy/private-key"))

;; Authorization is performed by sending the following variables into the request header: 

;; Key — Public API key. An example API key: 5a8808b25e3f59d8818d3fbc0ce993fbb82dcf90 

;; Sign — ALL POST data (param=val&param1=val1) signed by a secret key according to HMAC-SHA512 method. Your secret key and public keys can be generated from your account settings page. 

;; An additional security element must be passed into the post: 

;; nonce - All requests must also include a special nonce POST parameter with incrementing integer. The integer must always be greater than the previous requests nonce value. 

;; (http/post "http://pubapi.cryptsy.com/api.php?method=getinfo"
;;            {"Key" "9ce5ef061247770f79a6f6213b4e7c31cf655c3f"
;;             "nonce" (clj-time.core/now)})

;; (def public-key "9ce5ef061247770f79a6f6213b4e7c31cf655c3f")
;; (def private-key (slurp "/Users/joachim/.cryptsy/private-key"))


;; (defn cryptsy-private [method key secret-key &{:keys [query-params] 
;;                                                :or {query-params {}}}]
;;   (let [nonce (clj-time.coerce/to-long (clj-time.core/now))
;;         post-data (http/generate-query-string query-params) )])


;; ;;; or (http-request-for request-method http-url body) ???



;;   (json/read-str (:body (http/ (str "http://pubapi.cryptsy.com/api.php?method=" method)
;;                                {:query-params query-params} ))
;;                  :key-fn keyword))
;; (cryptsy "getinfo" )

;; Inputs: n/a 

;; Outputs: 

;; balances_available	Array of currencies and the balances availalbe for each
;; balances_hold	Array of currencies and the amounts currently on hold for open orders
;; servertimestamp	Current server timestamp
;; servertimezone	Current timezone for the server
;; serverdatetime	Current date/time on the server
;; openordercount	Count of open orders on your account


;; <?php

;; function api_query($method, array $req = array()) {
;;         // API settings
;;         $key = ''; // your API-key
;;         $secret = ''; // your Secret-key
 
;;         $req['method'] = $method;
;;         $mt = explode(' ', microtime());
;;         $req['nonce'] = $mt[1];
       
;;         // generate the POST data string
;;         $post_data = http_build_query($req, '', '&');

;;         $sign = hash_hmac("sha512", $post_data, $secret);
 
;;         // generate the extra headers
;;         $headers = array(
;;                 'Sign: '.$sign,
;;                 'Key: '.$key,
;;         );
 
;;         // our curl handle (initialize if required)
;;         static $ch = null;
;;         if (is_null($ch)) {
;;                 $ch = curl_init();
;;                 curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
;;                 curl_setopt($ch, CURLOPT_USERAGENT, 'Mozilla/4.0 (compatible; Cryptsy API PHP client; '.php_uname('s').'; PHP/'.phpversion().')');
;;         }
;;         curl_setopt($ch, CURLOPT_URL, 'https://www.cryptsy.com/api');
;;         curl_setopt($ch, CURLOPT_POSTFIELDS, $post_data);
;;         curl_setopt($ch, CURLOPT_HTTPHEADER, $headers);
;;         curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, FALSE);
 
;;         // run the query
;;         $res = curl_exec($ch);

;;         if ($res === false) throw new Exception('Could not get reply: '.curl_error($ch));
;;         $dec = json_decode($res, true);
;;         if (!$dec) throw new Exception('Invalid data received, please make sure connection is working and requested API exists');
;;         return $dec;
;; }
 
;; $result = api_query("getinfo");
