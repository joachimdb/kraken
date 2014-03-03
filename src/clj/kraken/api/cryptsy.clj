(ns kraken.api.cryptsy
  (:use [kraken.system]
        [kraken.model]
        [kraken.channels]
        [kraken.elastic]
        [pandect.core])
  (:require [clojure.core.async :as as]
            [clj-time.core :as tcore]
            [clj-time.coerce :as tcoerce]
            [clj-time.format :as tformat]
            [clj-http.client :as http]
            [clojure.data.json :as json]))

;; TODO: when trade channel is started, the las-id is set to the id of the most recent trade. This
;; means that when the system is (re)started, indexing begins with the last trade. This is not good
;; in the sense that data from before start time is lost. But it is good in the sense that this
;; prevents duplicate trades to be sent in between restarts, except when a restart occurs before a
;; new trade occurred. Although this is probably a rare event, it should be fixed (by adding a
;; last-trade-id to cfg on init)

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
  (let [ret (get (cryptsy-private "getinfo" public-key private-key) "return")
        available (into {} (remove #(= 0 (val %)) (get ret "balances_available")))]
    {:available (zipmap (keys (get ret "balances_available"))
                        (map #(if (string? %) (read-string %) %) (vals (get ret "balances_available"))))
     :held (zipmap (keys (get ret "balances_hold"))
                   (map #(if (string? %) (read-string %) %) (vals (get ret "balances_hold"))))
     :open-order-count (get ret "openordercount")
     :info-time (tcoerce/from-long (* 1000 (get ret "servertimestamp")))
     :server-time-zone (get ret "servertimezone")
     ;; :time (tcore/from-time-zone (tformat/parse (get ret "serverdatetime"))
     ;;                             (tcore/time-zone-for-id (get ret "servertimezone")))
     }))

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

(defn- market-trades 
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

(defn- market-orders 
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

(defn- my-trades
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

(defn- my-orders 
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

(defn- depth 
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

(defn- create-order
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


(defn- cancel-order 
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

(defn- calculate-fees
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

(defn- trade-channel* [public-key private-key market-id exchange-time-zone poll-interval last-trade-id control-channel error-channel]
  (let [last-id (atom (when last-trade-id (read-string last-trade-id)))]   
    (as/filter< (fn [trade] 
                  (let [id (read-string (:id trade))]
                    (when (or (nil? @last-id)
                              (> id @last-id))
                      (reset! last-id id))))
                (as/mapcat< reverse ;; trades come in most recent first
                            (rec-channel (fn [_] (market-trades public-key private-key market-id exchange-time-zone))
                                         nil
                                         poll-interval
                                         control-channel
                                         error-channel)))))

(defn- last-trade-id [system exchange-code market-code]
  (try (:id (first (trades system exchange-code
                           :query {:term {:market-code market-code}}
                           :sort {:time {:order "desc"}}
                           :size 1)))
       (catch Exception e nil)))

(defn- market-id [markets market-code] (:id (first (filter #(= (:market-code %) market-code) markets))))

(defn- start-trade-channel! [system component-id market-code]
  (let [control-channel (as/chan 1)]
    (as/tap (get-cfg system component-id :control) control-channel)
    (set-cfg system 
             component-id 
             {:trade-channels 
              {market-code 
               (as/map< #(apply mk-trade "cryptsy" market-code ((juxt :price :quantity :time :id) %))
                        (trade-channel* 
                         (get-cfg system component-id :public-key) 
                         (get-cfg system component-id :private-key)
                         (market-id (get-cfg system component-id :markets) market-code) 
                         (get-cfg system component-id :exchange-time-zone)
                         (get-cfg system component-id :poll-interval)
                         (last-trade-id system "cryptsy" market-code)
                         control-channel 
                         (as/map> (fn [e]
                                    :source component-id
                                    :level :error
                                    :msg (str "System " (vec (flatten [component-id])) " - " e)
                                    :exception e
                                    :time (tcore/now))
                                  (system-log-channel system))))}})))

(defn trade-channels [system component-id]
  (get-cfg system component-id :trade-channels))

;; (def poll-interval 5000)
;; (def control-channel (as/chan))
;; (def error-channel (as/chan))
;; (def tc (trade-channel (system) "DOGE/BTC" 5000 control-channel error-channel))
;; (as/>!! control-channel :start)
;; (as/take! tc (fn [v] (println "Got " v) (flush)))
;; (def t (as/<!! tc))
;; (as/>!! control-channel :close)

;; (as/take! error (fn [v] (println "Error " v) (flush)))

;; (defn depth-channel [public-key private-key market-id poll-interval control-channel error-channel]
;;   (rec-channel (fn [_] (depth public-key private-key market-id))
;;                  nil
;;                  poll-interval
;;                  control-channel
;;                  error-channel))


(defn- cryptsy-initial-config [info markets]
  {:balances (deep-merge (zipmap (keys (remove #(zero? (val %)) (:available info)))
                                         (map #(hash-map :unheld %)
                                              (vals (remove #(zero? (val %)) (:available info)))))
                                 (zipmap (keys (remove #(zero? (val %)) (:held info)))
                                         (map #(hash-map :held %)
                                              (vals (remove #(zero? (val %)) (:held info))))))
   :last-updated (:info-time info)
   :exchange-time-zone (:server-time-zone info)
   :open-order-count (:open-order-count info)
   :markets markets})

(defn- cryptsy-control-config []
  (let [control-channel (as/chan)]
    {:control-channel control-channel
     :control (as/mult control-channel)}))
;; (init-cryptsy (system) :cryptsy)
(defn- init-cryptsy [system id]
  (let [public-key (get-cfg system id :public-key)
        private-key (get-cfg system id :private-key)
        poll-interval (get-cfg system id :poll-interval)]
    (if (and public-key private-key poll-interval)
      (-> system 
          (set-cfg id (cryptsy-initial-config (get-info public-key private-key) (get-markets public-key private-key)))
          (set-cfg id (cryptsy-control-config))
          (set-cfg id {:poll-interval poll-interval})
          (start-trade-channel! id "DOGE/BTC"))
      (throw (Exception. "Cannot initialize cryptsy: keys not specified in system")))))

;; (def id :cryptsy)
;; (def public-key (get-cfg (system) id :public-key))
;; (def private-key (get-cfg (system) id :private-key))
;; (def poll-interval (get-cfg (system) id :poll-interval))

;; (def i (get-info public-key private-key))
;; (def m (get-markets public-key private-key))
;; (cryptsy-initial-config i m)

;; (def ret (get (cryptsy-private "getinfo" public-key private-key) "return"))


(defn- start-cryptsy [system id]
  (as/go (as/>! (get-cfg system id :control-channel) :start))
  system)

(defn- stop-cryptsy [system id]
  (as/go (as/>! (get-cfg system id :control-channel) :stop))
  system)

(defn- shutdown-cryptsy [system id]
  (as/go (as/>! (get-cfg system id :control-channel) :close))
  (as/close! (get-cfg system id :control-channel))
  system)

(defcomponent :cryptsy [:elastic]  
  ComponentP 
  (initial-config [this] (read-string (slurp (str (System/getProperty "user.home") "/.kraken/cryptsy/config.edn"))))
  (initialize [this system] (init-cryptsy system :cryptsy))
  (start [this system] (start-cryptsy system :cryptsy))
  (stop [this system] (stop-cryptsy system :cryptsy))
  (shutdown [this system] (shutdown-cryptsy system :cryptsy)))






;; (system)



