(ns kraken.model
  (:require [clj-time.format :as tformat]
            [clj-time.coerce :as tcoerce]
            [clojure.core.async :as as]))

;; TO DO: use loging instead of println

(defn primary-currency [^java.lang.String market-code]
  (.substring market-code 0 (.indexOf market-code "/")))

(defn secondary-currency [^java.lang.String market-code]
  (.substring market-code (inc (.indexOf market-code "/"))))

(defrecord Tick [last-trade-price last-volume volume24 high24 low24 
                 ;; spread, ask-volume, ...
                 ])
(defn mk-tick [last-trade-price last-volume volume24 high24 low24] 
  (Tick. last-trade-price last-volume volume24 high24 low24))

(defrecord Trade [^java.lang.Double price 
                  ^java.lang.Double volume
                  ^org.joda.time.DateTime time])

(defn mk-trade [price volume time]
  (Trade. price volume time))

(defrecord Order [^java.lang.Double price 
                  ^java.lang.Double volume])

(defn mk-order [price volume]
  (Order. price volume))

(defrecord OrderBook [sell-orders
                      buy-orders])

(defn mk-order-book [sell-orders buy-orders]
  ;; TO DO: orderbook itself not very interesting => compute spread/depth measures
  (OrderBook. sell-orders buy-orders))

;;;;;;; ----- Meta Model ------

(defn deep-merge
  "Recursively merges maps. If keys are not maps, the last value wins."
  [& vals]
  (if (every? map? vals)
    (apply merge-with deep-merge vals)
    (last vals)))

(defprotocol ComponentP
  (initialize [this system])
  (stop [this system])
  (start [this system]))

(defrecord Component [id f-init f-start f-stop]
  ;; TO DO: in case of dependencies, it's better to work through the list of dependencies sequentially (i.e. functionally). We then need to add a record slot for depencies.
  ComponentP
  (initialize [this system] (f-init system id))
  (start [this system] (f-start system id))
  (stop [this system] (f-stop system id)))

(defn mk-component [id f-init f-start f-stop]
  (Component. id f-init f-start f-stop))

(def +system+ (atom nil))

(defn system []
  @+system+)

(defmacro defcomponent [id [:as args] & {:keys [init start stop dependencies]
                                         :or {init (fn [_ system] system)
                                              start (fn [_ system] system)
                                              stop(fn [_ system] system)
                                              dependencies #{}}}]
  `(swap! +system+ 
          #(update-in 
            (deep-merge %
                        (assoc-in {}
                                  (flatten [:components ~id]) 
                                  {:instance (mk-component ~id ~init ~start ~stop)
                                   :status nil}))
            [:components :system :dependencies]
            clojure.set/union 
            ~(disj (conj dependencies id) :system))))

(defn dependencies [system component-id]
  (get-in system (flatten [:components component-id :dependencies])))

(defn status [system component-id]
  (get-in system (flatten [:components component-id :status])))
(defn running? [system component-id]
  (= :running (status system component-id)))
(defn starting? [system component-id]
  (= :starting (status system component-id)))
(defn stopped? [system component-id]
  (= :stopped (status system component-id)))
(defn stopping? [system component-id]
  (= :stopping (status system component-id)))
(defn initialized? [system component-id]
  (= :initialized (status system component-id)))
(defn initializing? [system component-id]
  (= :initializing (status system component-id)))

(defn set-status [system component-id status]
  (assoc-in system (flatten [:components component-id :status]) status))

(defn get-cfg [system component-id & keys]
  (get-in system (flatten [component-id keys])))
(defn configure [system component-id cfg]
  (assoc-in system (flatten [component-id]) cfg))

(defn component 
  ([id] (component @+system+ id))
  ([system id]
     (get-in system (flatten [:components id :instance]))))

(defn stop-component [system component-id]
  (if (or (stopped? system component-id)
          (stopping? system component-id))
    system
    (try (set-status 
          (stop (component system component-id)
                (reduce stop-component
                        (set-status system component-id :stopping)
                        (dependencies system component-id)))
          component-id
          :stopped)
         (catch Exception e 
           (set-status system component-id
                       {:failed :stopping
                        :exception e})))))

(defn initialize-component [system component-id]
  (if (or (initialized? system component-id)
          (initializing? system component-id))
    system
    (let [dependencies (dependencies system component-id)
          fresh-system (loop [s (set-status system component-id :initializing) 
                              d dependencies]
                         (if (empty? d)
                           s
                           (recur (initialize-component s (first d))
                                  (rest d))))]
      (println "Initializing component " component-id)
      (if (empty? dependencies)
        (println "   No dependencies")
        (do (println "   " (count dependencies) " dependencies") 
            (doseq [d dependencies]
              (println "   - dependency: " d ", status: " (status fresh-system d)))))
      (if (every? (partial initialized? fresh-system) dependencies)
        (try (set-status 
              (initialize (component fresh-system component-id) fresh-system)
              component-id
              :initialized)
             (catch Exception e
               (def si fresh-system)
               (def ci component-id)
               (set-status fresh-system component-id 
                           e)))
        fresh-system))))

(defn start-component [system component-id]
  (if (or (running? system component-id)
          (starting? system component-id)
          (not (every? (partial running? system) (dependencies system component-id))))
    system
    (try (set-status 
          (start 
           (component system component-id)
           (reduce start-component
                   (set-status (initialize-component system component-id) component-id :starting)
                   (dependencies system component-id))))
         (catch Exception e 
           (set-status system component-id
                       {:failed :starting
                        :exception e})))))
  
(defcomponent :system []
  :init (fn [system id] (initialize-component system id))
  :start (fn [system id] (start-component system id))
  :stop (fn [system id] (stop-component system id)))

