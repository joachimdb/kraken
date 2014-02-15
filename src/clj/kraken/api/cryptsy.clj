(ns kraken.api.cryptsy
  (:use [kraken.model]
        [kraken.channels]
        [pandect.core])
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

                
;; (reset-market-ids!)

(defn orders 
  "Same as market-data, but doesn't return values for any trade related keys"
  [& market-codes]
  (if (empty? market-codes)
    (let [all-orders (get (cryptsy-public "orderdata") "return")]
      (zipmap (keys all-orders)
              (map parse-market-data (vals all-orders))))
    (zipmap market-codes
            (map (fn [market-code]
                   (let [md (val (first (get (cryptsy-public "singleorderdata" :query-params {:marketid (market-codes->ids market-code)})
                                             "return")))]
                     (dissoc (parse-market-data md)
                             :recent-trades :last-trade-time :last-trade-price :volume)))
                 market-codes))))
;; (orders "LTC/BTC")

(defn order-book-channel [market-code & {:keys [interval]
                                         :or {interval (* 1000 60)}}]
  (as/filter< identity ;; filter out on-fails (false)
              (polling-channel (fn []
                                 (let [orders (get (orders market-code) market-code)]
                                   (mk-order-book (map #(mk-order (:price %) (:quantity %))
                                                       (:sell-orders orders))
                                                  (map #(mk-order (:price %) (:volume %))
                                                       (:buy-orders orders)))))
                               :interval interval
                               :on-fail false)))


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
      :volume 1765529079.68198780
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
    (let [all-mdata (get-in (cryptsy-public "marketdatav2") ["return" "markets"])]
      (zipmap (keys all-mdata)
              (map parse-market-data (vals all-mdata))))
    (zipmap market-labels
            (map #(parse-market-data 
                   (val 
                    (first 
                     (get-in (cryptsy-public "singlemarketdata" :query-params {:marketid %})
                             ["return" "markets"]))))
                 (map market-codes->ids market-labels)))))

(defn- trades [market-data]
  (map #(apply mk-trade ((juxt :prive :quantity :time) %))
       (:recent-trades market-data)))

(defn- order-book [market-data]
  (mk-order-book (map #(mk-order (:price %) (:quantity %))
                      (:sell-orders market-data))
                 (map #(mk-order (:price %) (:volume %))
                      (:buy-orders market-data))))

;; (defn- ticks [market-data] ... )

(defn- market-data-channel [market-label & {:keys [interval]
                                            :or {interval (* 1000 60)}}]
  (recursive-channel (fn [prev] 
                       (get (market-data market-label) market-label))
                     nil
                     :interval interval
                     :on-fail false))

;; Market-data returns information that in kraken is spread over ticks, order-books and trades.
;; Here we could provide a publication factory for publishing trades, ticks and order-books.
;; In kraken we could provide a market-data channel that combines trades, ticks and order-books channels.

;; ;;; private api

(defn- cryptsy-private [method public-key private-key &{:keys [input] 
                                                        :or {input {}}}]
  (let [query-params (merge input {:method method
                                   :nonce (tcoerce/to-long (tcore/now))})
        query-string (http/generate-query-string query-params)
        signature (sha512-hmac query-string private-key)
        body (json/read-str (:body (http/post "https://api.cryptsy.com/api"
                                              {:form-params query-params
                                               :headers {"sign" signature
                                                         "key" public-key}})))]
    (when (= "0" (get body "success"))
      (throw (Exception. (get body "error"))))
    (get body "return")))

(defn- get-info 
  "Method: getinfo 

   Inputs: n/a 

   Outputs: 

   :available	Array of currencies and the balances availalbe for each
   :held	Array of currencies and the amounts currently on hold for open orders
   :info-time	Time at server when info was served (in UTC)
   :server-time-zone Time zone at server
   :open-order-count	Count of open orders on your account
"
  [public-key private-key]
  (let [ret (cryptsy-private "getinfo" public-key private-key)]
    {:available (zipmap (keys (get ret "balances_available"))
                        (map read-string (vals (get ret "balances_available"))))
     :held (zipmap (keys (get ret "balances_hold"))
                   (map read-string (vals (get ret "balances_hold"))))
     :open-order-count (get ret "openordercount")
     :info-time (tcoerce/from-long (* 1000 (get ret "servertimestamp")))
     :server-time-zone (get ret "servertimezone")
     ;; :time (tcore/from-time-zone (tformat/parse (get ret "serverdatetime"))
     ;;                             (tcore/time-zone-for-id (get ret "servertimezone")))
     }))
;; (def info (get-info public-key private-key))

(defn- get-markets 
  "Method: getmarkets 

   Inputs: n/a 

   Outputs: Array of Active Markets 
   
   cryptsy-id	String value representing a market
   market-code	Name for this market, for example: AMC/BTC
   volume24	24 hour trading volume in this market
   last-trade-price	Last trade price for this market
   high24	24 hour highest trade price in this market
   low24	24 hour lowest trade price in this market
"   
  [public-key private-key]
  (let [ret (cryptsy-private "getmarkets" public-key private-key)]
    (map #(hash-map :cryptsy-id (get % "marketid")
                    :market-code (get % "label")
                    :volume24 (read-string (get % "current_volume"))
                    :last-trade-price (read-string (get % "last_trade"))
                    :high24 (read-string (get % "high_trade"))
                    :low24 (read-string (get % "low_trade")))
         ret)))
;; (def markets (get-markets))

;; (def public-key "9ce5ef061247770f79a6f6213b4e7c31cf655c3f")
;; (def private-key (slurp (str (System/getProperty "user.home") "/.cryptsy/private-key")))
;; (read-string (slurp (str (System/getProperty "user.home") "/.cryptsy/config.edn")))
;; (def info (get-info public-key private-key))
;; (def markets (get-markets publick-key private-key))

(defn init-cryptsy [system id]
  (let [public-key (get-cfg system id :public-key)
        private-key (get-cfg system id :private-key)]
    (if (and public-key private-key)
      (let [info (get-info public-key private-key)
            markets (get-markets public-key private-key)]
        (configure system id
                   {:balances
                    (deep-merge (zipmap (keys (remove #(zero? (val %)) (:available info)))
                                        (map #(hash-map :unheld %)
                                             (vals (remove #(zero? (val %)) (:available info)))))
                                (zipmap (keys (remove #(zero? (val %)) (:held info)))
                                        (map #(hash-map :held %)
                                             (vals (remove #(zero? (val %)) (:held info))))))
                    :last-updated (:info-time info)
                    :exchange-time-zone (:server-time-zone info)
                    :open-order-count (:open-order-count info)
                    :markets markets}))
      (throw (Exception. "Cannot initialize cryptsy: keys not specified in system")))))
;; (init-cryptsy kraken.model/ts [:exchanges :cryptsy])

(defcomponent [:exchanges :cryptsy] []
  ;; Requires  public and private-keys
  :init init-cryptsy  
  ;; we could start an update thread and stop it here if wished
  )

(defn- my-transactions 
  "Method: mytransactions 
   
   Inputs: n/a 
   
   Outputs: Array of Deposits and Withdrawals on your account 
   
   currency	Name of currency account
   time  	The timestamp the activity posted
   timezone	Server timezone
   type	        Type of activity. (Deposit / Withdrawal)
   address	Address to which the deposit posted or Withdrawal was sent
   amount	Amount of transaction (Not including any fees)
   fee	        Fee (If any) Charged for this Transaction (Generally only on Withdrawals)
   trxid	Network Transaction ID (If available)
"   
  []
  (let [ret (cryptsy-private "mytransactions" public-key private-key)]
    (map #(hash-map :fee (read-string (get % "fee"))
                    :currency (get % "currency")
                    :amount (read-string (get % "amount"))
                    :type (get % "type")
                    :address (get % "address")
                    :trxid (get % "trxid")
                    :time (tcoerce/from-long (* 1000 (get % "timestamp")))
                    :time-zone (get ret "timezone"))
         ret)))

(defn market-trades 
  "Method: markettrades 
   
   Inputs:
   
   market-code	Market code for which you are querying
   
   
   Outputs: Array of last 1000 Trades for this Market, in Date Decending Order 
   
   cryptsy-id	A unique ID for the trade
   time	        time trade occurred
   price	The price the trade occurred at
   quantity	Quantity traded
   total	Total value of trade (tradeprice * quantity)
   initial-order-type	The type of order which initiated this trade (\"Buy\"/\"Sell\")
"   
  [exchange-info market-code]
  (let [ret (cryptsy-private "markettrades" public-key private-key :input {"marketid" (market-codes->ids market-code)})]
    (map #(hash-map :cryptsy-id (get % "tradeid")
                    :currency (get % "currency")
                    :amount (read-string (get % "amount"))
                    :type (get % "type")
                    :address (get % "address")
                    :trxid (get % "trxid")
                    :time (tcoerce/from-long (* 1000 (get % "timestamp")))
                    :time-zone (get ret "timezone")))))
;; (def market-code "DOGE/BTC")
;; (def ret (cryptsy-private "markettrades" public-key private-key :input {"marketid" (market-codes->ids market-code)}))
;; (first ret)
;; info

;;    Method: marketorders 
   
;;    Inputs:
   
;;    marketid	Market ID for which you are querying
   
   
;;    Outputs: 2 Arrays. First array is sellorders listing current open sell orders ordered price ascending. Second array is buyorders listing current open buy orders ordered price descending. 
   
;;    sellprice	If a sell order, price which order is selling at
;;    buyprice	If a buy order, price the order is buying at
;;    quantity	Quantity on order
;;    total	Total value of order (price * quantity)
   
   
;;    Method: mytrades 
   
;;    Inputs:
   
;;    marketid	Market ID for which you are querying
;;    limit	(optional) Limit the number of results. Default: 200
   
   
;;    Outputs: Array your Trades for this Market, in Date Decending Order 
   
;;    tradeid	An integer identifier for this trade
;;    tradetype	Type of trade (Buy/Sell)
;;    datetime	Server datetime trade occurred
;;    tradeprice	The price the trade occurred at
;;    quantity	Quantity traded
;;    total	Total value of trade (tradeprice * quantity) - Does not include fees
;;    fee	Fee Charged for this Trade
;;    initiate_ordertype	The type of order which initiated this trade
;;    order_id	Original order id this trade was executed against
   
   
;;    Method: allmytrades 
   
;;    Inputs: n/a 
   
;;    Outputs: Array your Trades for all Markets, in Date Decending Order 
   
;;    tradeid	An integer identifier for this trade
;;    tradetype	Type of trade (Buy/Sell)
;;    datetime	Server datetime trade occurred
;;    marketid	The market in which the trade occurred
;;    tradeprice	The price the trade occurred at
;;    quantity	Quantity traded
;;    total	Total value of trade (tradeprice * quantity) - Does not include fees
;;    fee	Fee Charged for this Trade
;;    initiate_ordertype	The type of order which initiated this trade
;;    order_id	Original order id this trade was executed against
   
   
;;    Method: myorders 
   
;;    Inputs:
   
;;    marketid	Market ID for which you are querying
   
   
;;    Outputs: Array of your orders for this market listing your current open sell and buy orders. 
   
;;    orderid	Order ID for this order
;;    created	Datetime the order was created
;;    ordertype	Type of order (Buy/Sell)
;;    price	The price per unit for this order
;;    quantity	Quantity remaining for this order
;;    total	Total value of order (price * quantity)
;;    orig_quantity	Original Total Order Quantity
   
   
;;    Method: depth 
   
;;    Inputs:
   
;;    marketid	Market ID for which you are querying
   
   
;;    Outputs: Array of buy and sell orders on the market representing market depth. 
   
;;    Output Format is:
;;    array(
;;      'sell'=>array(
;;        array(price,quantity), 
;;        array(price,quantity),
;;        ....
;;      ), 
;;      'buy'=>array(
;;        array(price,quantity), 
;;        array(price,quantity),
;;        ....
;;      )
;;    )
   
   
;;    Method: allmyorders 
   
;;    Inputs: n/a 
   
;;    Outputs: Array of all open orders for your account. 
   
;;    orderid	Order ID for this order
;;    marketid	The Market ID this order was created for
;;    created	Datetime the order was created
;;    ordertype	Type of order (Buy/Sell)
;;    price	The price per unit for this order
;;    quantity	Quantity remaining for this order
;;    total	Total value of order (price * quantity)
;;    orig_quantity	Original Total Order Quantity
   
   
;;    Method: createorder 
   
;;    Inputs:
   
;;    marketid	Market ID for which you are creating an order for
;;    ordertype	Order type you are creating (Buy/Sell)
;;    quantity	Amount of units you are buying/selling in this order
;;    price	Price per unit you are buying/selling at
   
   
;;    Outputs: 
   
;;    orderid	If successful, the Order ID for the order which was created
   
   
;;    Method: cancelorder 
   
;;    Inputs:
   
;;    orderid	Order ID for which you would like to cancel
   
   
;;    Outputs: n/a. If successful, it will return a success code. 
   
;;    Method: cancelmarketorders 
   
;;    Inputs:
   
;;    marketid	Market ID for which you would like to cancel all open orders
   
   
;;    Outputs: 
   
;;    return	Array for return information on each order cancelled
   
   
;;    Method: cancelallorders 
   
;;    Inputs: N/A 
   
;;    Outputs: 
   
;;    return	Array for return information on each order cancelled
   
   
;;    Method: calculatefees 
   
;;    Inputs:
   
;;    ordertype	Order type you are calculating for (Buy/Sell)
;;    quantity	Amount of units you are buying/selling
;;    price	Price per unit you are buying/selling at
   
   
;;    Outputs: 
   
;;    fee	The that would be charged for provided inputs
;;    net	The net total with fees
   
   
;;    Method: generatenewaddress 
   
;;    Inputs: (either currencyid OR currencycode required - you do not have to supply both)
   
;;    currencyid	Currency ID for the coin you want to generate a new address for (ie. 3 = BitCoin)
;;    currencycode	Currency Code for the coin you want to generate a new address for (ie. BTC = BitCoin)
   
   
;;    Outputs: 
   
;;    address	The new generated address
   
   
