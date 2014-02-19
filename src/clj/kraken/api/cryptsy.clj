(ns kraken.api.cryptsy
  (:use [kraken.system]
        [kraken.model]
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

(defn orders 
  "Same as market-data, but doesn't return values for any trade related keys"
  [& market-ids]
  (if (empty? market-ids)
    (let [all-orders (get (cryptsy-public "orderdata") "return")]
      (zipmap (keys all-orders)
              (map parse-market-data (vals all-orders))))
    (zipmap market-ids
            (map (fn [market-id]
                   (let [md (val (first (get (cryptsy-public "singleorderdata" :query-params {:marketid market-id})
                                             "return")))]
                     (dissoc (parse-market-data md)
                             :recent-trades :last-trade-time :last-trade-price :volume)))
                 market-ids))))
;; (orders "LTC/BTC")

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
 [& market-ids]
 (if (empty? market-ids)
    (let [all-mdata (get-in (cryptsy-public "marketdatav2") ["return" "markets"])]
      (zipmap (keys all-mdata)
              (map parse-market-data (vals all-mdata))))
    (zipmap market-ids
            (map #(parse-market-data 
                   (val 
                    (first 
                     (get-in (cryptsy-public "singlemarketdata" :query-params {:marketid %})
                             ["return" "markets"]))))
                 market-ids))))

(defn- trades [market-data]
  (map #(apply mk-trade ((juxt :prive :quantity :time) %))
       (:recent-trades market-data)))

(defn- order-book [market-data]
  (mk-order-book (map #(mk-order (:price %) (:quantity %))
                      (:sell-orders market-data))
                 (map #(mk-order (:price %) (:volume %))
                      (:buy-orders market-data))))

;; (defn- ticks [market-data] ... )

;; ;;; private api

(defn- cryptsy-private [method public-key private-key &{:keys [params] 
                                                        :or {params {}}}]
  (let [query-params (merge params {:method method
                                    :nonce (tcoerce/to-long (tcore/now))})
        query-string (http/generate-query-string query-params)
        signature (sha512-hmac query-string private-key)
        body (json/read-str (:body (http/post "https://api.cryptsy.com/api"
                                              {:form-params query-params
                                               :headers {"sign" signature
                                                         "key" public-key}})))]
    (when (= "0" (get body "success"))
      (throw (Exception. (get body "error"))))
    (dissoc body "success")))

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
  (let [ret (get (cryptsy-private "getinfo" public-key private-key) "return")]
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
   
   id	        String value representing a market
   market-code	Name for this market, for example: AMC/BTC
   volume24	24 hour trading volume in this market
   last-trade-price	Last trade price for this market
   high24	24 hour highest trade price in this market
   low24	24 hour lowest trade price in this market
"   
  [public-key private-key]
  (let [ret (get (cryptsy-private "getmarkets" public-key private-key) "return")]
    (map #(hash-map :id (get % "marketid")
                    :market-code (get % "label")
                    :volume24 (read-string (get % "current_volume"))
                    :last-trade-price (read-string (get % "last_trade"))
                    :high24 (read-string (get % "high_trade"))
                    :low24 (read-string (get % "low_trade")))
         ret)))
;; (def markets (get-markets))

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
  [public-key private-key]
  (let [ret (get (cryptsy-private "mytransactions" public-key private-key) "return")]
    (map #(hash-map :fee (read-string (get % "fee"))
                    :currency (get % "currency")
                    :amount (read-string (get % "amount"))
                    :type (get % "type")
                    :address (get % "address")
                    :trxid (get % "trxid")
                    :time (tcoerce/from-long (* 1000 (get % "timestamp")))
                    :time-zone (get ret "timezone"))
         ret)))
;; (my-transactions public-key private-key)

(defn market-trades 
  "Method: markettrades 
   
   Params:
   
   market-id	Market code for which you are querying
   
   Outputs: Array of last 1000 Trades for this Market, in Date Decending Order 
   
   id	        A unique ID for the trade
   time	        time trade occurred
   price	The price the trade occurred at
   quantity	Quantity traded
   initial-order-type	The type of order which initiated this trade (\"Buy\"/\"Sell\")
"   
  [public-key private-key market-id exchange-time-zone]
  (let [ret (get (cryptsy-private "markettrades" public-key private-key :params {"marketid" market-id})
                 "return")]
    (map (fn [trade]
           {:id (get trade "tradeid")
            :time (tcore/from-time-zone (tformat/parse (get trade "datetime"))
                                        (tcore/time-zone-for-id exchange-time-zone))
            :price (read-string (get trade "tradeprice"))
            :quantity (read-string (get trade "quantity"))})
         ret)
    ))
;; (market-trades public-key
;;                private-key
;;                market-id
;;                exchange-time-zone)

(defn market-orders 
  "Method: marketorders 
   
   Inputs:
   
   marketid	Market ID for which you are querying
   
   
   Outputs: 2 Arrays. First array is sellorders listing current open sell orders ordered price ascending. Second array is buyorders listing current open buy orders ordered price descending. 
   
   sellprice	If a sell order, price which order is selling at
   buyprice	If a buy order, price the order is buying at
   quantity	Quantity on order
   total	Total value of order (price * quantity)
"
  [public-key private-key market-id]
  (let [ret (get (cryptsy-private "marketorders" public-key private-key :params {:marketid market-id})
                 "return")]
    {:sell-orders (map #(hash-map :price (read-string (get % "sellprice"))
                                  :quantity (read-string (get % "quantity")))
                       (get ret "sellorders"))
     :buy-orders (map #(hash-map :price (read-string (get % "buyprice"))
                                  :quantity (read-string (get % "quantity")))
                       (get ret "buyorders"))}))
;; (market-orders public-key private-key market-id)

(defn my-trades
  "Method: mytrades 
   
   Inputs:
   
   marketid	Market ID for which you are querying
   limit	(optional) Limit the number of results. Default: 200
   
   
   Outputs: Array your Trades for this Market, in Date Decending Order 
   
   id	        cryptsy id of trade
   order-id     cryptsy id of order resulting in trade
   type 	Type of trade (Buy/Sell)
   time	        UTC datetime trade occurred
   price	The price the trade occurred at
   quantity	Quantity traded
   fee	        Fee Charged for this Trade
"   
  ([public-key private-key market-id exchange-time-zone] (my-trades public-key private-key market-id exchange-time-zone 200))
  ([public-key private-key market-id exchange-time-zone limit] 
     (let [ret (get (cryptsy-private "mytrades" public-key private-key :params {:marketid market-id :limit limit})
                    "return")]
       (map #(hash-map :type (get % "tradetype")
                       :price (read-string (get % "tradeprice"))
                       :fee (read-string (get % "fee"))
                       :quantity (read-string (get % "quantity"))
                       :id (get % "tradeid")
                       :order-id (get % "order_id")
                       :time (tcore/from-time-zone (tformat/parse (get % "datetime"))
                                                   (tcore/time-zone-for-id exchange-time-zone)))
            ret))))
;; (my-trades public-key private-key market-id exchange-time-zone)

(defn my-orders 
  "Method: myorders 
   
   Inputs:
   
   marketid	Market ID for which you are querying
   
   
   Outputs: Array of your orders for this market listing your current open sell and buy orders. 
   
   id	        cryptsy ID for this order
   time	        Datetime the order was created
   type	        Type of order (Buy/Sell)
   price	The price per unit for this order
   quantity	Quantity remaining for this order
   original-quantity	Original Total Order Quantity
"
  [public-key private-key market-id exchange-time-zone]
  (let [ret (get (cryptsy-private "myorders" public-key private-key :params {"marketid" market-id})
                 "return")]
    (map #(hash-map :id (get % "orderid")
                    :time (tcore/from-time-zone (tformat/parse (get % "created"))
                                                (tcore/time-zone-for-id exchange-time-zone))
                    :type (get % "ordertype")
                    :price (read-string (get % "price"))
                    :quantity (read-string (get % "quantity"))
                    :original-quantity (read-string (get % "orig_quantity")))
         ret)))
;; (my-orders public-key private-key market-id exchange-time-zone)

(defn depth 
  "Method: depth 
   
   Inputs:
   
   marketid	Market ID for which you are querying
   
   
   Outputs: Array of buy and sell orders on the market representing market depth. 
   
   Output Format is:
  {:sell-orders [{:price ..., :quantity ...} ...]
   :buy-orders [{:price ..., :quantity ...} ...]}
"
  [public-key private-key market-id]
  (let [ret (get (cryptsy-private "depth" public-key private-key :params {:marketid market-id})
                 "return")]
    {:sell-orders (map #(hash-map :price (read-string (first %))
                                  :quantity (read-string (second %)))
                       (get ret "sell"))
     :buy-orders (map #(hash-map :price (read-string (first %))
                                 :quantity (read-string (second %)))
                      (get ret "buy"))}))
;; (def public-key (get-cfg (system) :cryptsy :public-key))
;; (def private-key (get-cfg (system) :cryptsy :private-key))
;; (def d (depth public-key
;;               private-key
;;               (market-id (get-cfg (system) :cryptsy :markets) "DOGE/BTC")))
;; (first (:buy-orders d))

(defn create-order
  "Method: createorder 
   
   Inputs:
   
   marketid	Market ID for which you are creating an order for
   ordertype	Order type you are creating (Buy/Sell)
   quantity	Amount of units you are buying/selling in this order
   price	Price per unit you are buying/selling at
   
   
   Outputs: 
   
   the Order ID for the order which was created
"
  [public-key private-key market-id order-type quantity price]
  (let [body (cryptsy-private "createorder" public-key private-key
                              :params {:marketid market-id
                                       :ordertype order-type
                                       :quantity quantity
                                       :price price})]
    ;; TO DO: replace println by log
    (println (get body "moreinfo"))
    (get body "orderid")))
;; (def order-id (create-order public-key private-key market-id "Sell" 50000 0.00000300))
;; => order-id
;; (create-order public-key private-key market-id "Sell" 500000 0.00000300)
;; => exception (Insufficient funds)


(defn cancel-order 
  "Method: cancelorder 
   
   Inputs:
   
   orderid	Order ID for which you would like to cancel
   
   Outputs: true
"
  [public-key private-key order-id]
  (let [body (cryptsy-private "cancelorder" public-key private-key :params {:orderid order-id})]
    (println (get body "return"))
    true))
;; (cancel-order public-key private-key order-id)

(defn calculate-fees
  "Method: calculatefees 
   
   Inputs:
   
   order-type	Order type you are calculating for (Buy/Sell)
   quantity	Amount of units you are buying/selling
   price	Price per unit you are buying/selling at
   
   
   Outputs: 
   
   fee	The that would be charged for provided inputs
   net	The net total with fees
"
  [public-key private-key order-type quantity price]
  (let [ret (get (cryptsy-private "calculatefees" public-key private-key 
                                  :params {:ordertype order-type
                                           :quantity quantity
                                           :price price})
                 "return")]
    {:fee (read-string (get ret "fee"))
     :net (read-string (get ret "net"))}))
;; (calculate-fees public-key private-key "Sell" 50000 0.00000300)


;; TO DO: implement remaining endpoints below

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
   
   
;;    Method: cancelmarketorders 
   
;;    Inputs:
   
;;    marketid	Market ID for which you would like to cancel all open orders
   
   
;;    Outputs: 
   
;;    return	Array for return information on each order cancelled
   
   
;;    Method: cancelallorders 
   
;;    Inputs: N/A 
   
;;    Outputs: 
   
;;    return	Array for return information on each order cancelled
   
   
;;    Method: generatenewaddress 
   
;;    Inputs: (either currencyid OR currencycode required - you do not have to supply both)
   
;;    currencyid	Currency ID for the coin you want to generate a new address for (ie. 3 = BitCoin)
;;    currencycode	Currency Code for the coin you want to generate a new address for (ie. BTC = BitCoin)
   
   
;;    Outputs: 
   
;;    address	The new generated address

(defn trade-channel [public-key private-key market-id exchange-time-zone poll-interval control-channel error-channel]
  (let [last-id (atom nil)]   
    (as/map< #(apply mk-trade ((juxt :price :quantity :time) %))
             (as/filter< (fn [trade] 
                           (let [id (read-string (:id trade))]
                             (when (or (nil? @last-id)
                                       (> id @last-id))
                               (reset! last-id id))))
                         (as/mapcat< identity
                                     (rec-channel (fn [_] (market-trades public-key private-key market-id exchange-time-zone))
                                                  nil
                                                  poll-interval
                                                  control-channel
                                                  error-channel))))))
;; (def poll-interval 5000)
;; (def control-channel (as/chan))
;; (def error-channel (as/chan))
;; (def tc (trade-channel (system) "DOGE/BTC" 5000 control-channel error-channel))
;; (as/>!! control-channel :start)
;; (as/take! tc (fn [v] (println "Got " v) (flush)))
;; (def t (as/<!! tc))
;; (as/>!! control-channel :close)

;; (as/take! error (fn [v] (println "Error " v) (flush)))

(defn depth-channel [public-key private-key market-id poll-interval control-channel error-channel]
  (rec-channel (fn [_] (depth public-key private-key market-id))
                 nil
                 poll-interval
                 control-channel
                 error-channel))

(defn- market-id [markets market-code] (:id (first (filter #(= (:market-code %) market-code) markets))))

(defn init-cryptsy [system id]
  (let [public-key (get-cfg system id :public-key)
        private-key (get-cfg system id :private-key)]
    (if (and public-key private-key)
      (let [info (get-info public-key private-key)
            markets (get-markets public-key private-key)
            balances (deep-merge (zipmap (keys (remove #(zero? (val %)) (:available info)))
                                         (map #(hash-map :unheld %)
                                              (vals (remove #(zero? (val %)) (:available info)))))
                                 (zipmap (keys (remove #(zero? (val %)) (:held info)))
                                         (map #(hash-map :held %)
                                              (vals (remove #(zero? (val %)) (:held info))))))
            exchange-time-zone (:server-time-zone info)
            control-channel (as/chan)
            control (as/mult control-channel)
            doge-trade-control-channel (as/chan 1)
            doge-depth-control-channel (as/chan 1)
            error-channel (system-log-channel system)]
        (as/tap control doge-trade-control-channel)
        (as/tap control doge-depth-control-channel)
        (set-cfg system id
                 {:balances balances
                  :last-updated (:info-time info)
                  :exchange-time-zone exchange-time-zone
                  :open-order-count (:open-order-count info)
                  :markets markets
                  :control-channel control-channel
                  :doge-trade-channel (trade-channel public-key private-key (market-id markets "DOGE/BTC") exchange-time-zone 10000 doge-trade-control-channel error-channel)
                  :doge-depth-channel (depth-channel public-key private-key (market-id markets "DOGE/BTC") 60000 doge-depth-control-channel error-channel)})) 
      (throw (Exception. "Cannot initialize cryptsy: keys not specified in system")))))


(defn start-cryptsy [system id]
  (as/go (as/>! (get-cfg system id :control-channel) :start))
  system)

(defn stop-cryptsy [system id]
  (as/go (as/>! (get-cfg system id :control-channel) :stop))
  system)

(defn shutdown-cryptsy [system id]
  (as/go (as/>! (get-cfg system id :control-channel) :close))
  (as/close! (get-cfg system id :control-channel))
  system)

(defcomponent :cryptsy []  
  ComponentP 
  (initial-config [this] (read-string (slurp (str (System/getProperty "user.home") "/.kraken/cryptsy/config.edn"))))
  (initialize [this system] (init-cryptsy system :cryptsy))
  (start [this system] (start-cryptsy system :cryptsy))
  (stop [this system] (stop-cryptsy system :cryptsy))
  (shutdown [this system] (shutdown-cryptsy system :cryptsy)))




;; (system)



