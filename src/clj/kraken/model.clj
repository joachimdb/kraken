(ns kraken.model
  (:require [clj-time.format :as tformat]
            [clj-time.coerce :as tcoerce]
            [clj-time.core :as tcore]
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
  (start [this system])
  (stop [this system])
  (shutdown [this system]))

(defrecord Component [id f-init f-start f-stop f-shutdown]
  ComponentP
  (initialize [this system] (f-init system id))
  (start [this system] (f-start system id))
  (stop [this system] (f-stop system id))
  (shutdown [this system] (f-shutdown system id)))

(defn mk-component [id f-init f-start f-stop f-shutdown]
  (Component. id f-init f-start f-stop f-shutdown))

(def +system+ (atom {}))

(defn system []
  @+system+)

(defmacro defcomponent [id [:as args] & {:keys [init start stop shutdown dependencies]
                                         :or {init (fn [_ system] system)
                                              start (fn [_ system] system)
                                              stop (fn [_ system] system)
                                              shutdown (fn [_ system] system)
                                              dependencies #{}}}]
  `(swap! +system+ 
          #(update-in 
            (deep-merge %
                        (assoc-in {}
                                  (flatten [:components ~id]) 
                                  {:instance (mk-component ~id ~init ~start ~stop ~shutdown)
                                   :status :down}))
            [:components :system :dependencies]
            clojure.set/union 
            ~(disj (conj dependencies id) :system))))

(defn dependencies [system component-id]
  (get-in system (flatten [:components component-id :dependencies])))

(defn system-error-channel [s]
  (:error-channel s))
(defn set-system-error-channel [s ec]
  (assoc s :error-channel ec))
(defn error [system component-id & rest]
  (as/put! (system-error-channel system) {:source component-id
                                          :error rest
                                          :time (tcore/now)}))

(defn system-log-channel [s]
  (:log-channel s))
(defn set-system-log-channel [s lc]
  (assoc s :log-channel lc))
(defn log [system component-id & rest]
  (as/put! (system-log-channel system) {:source component-id
                                        :message rest
                                        :time (tcore/now)}))

;; TODO: cleanup status mess and work out order of things:
;;               up        running          stopped        down
;; initialize    --      rec shutdown    rec shutdown      ->up
;; start     ->running       --          ->runnning      rec initialize
;; stop          --       ->stopped           --            --
;; shutdown   ->down       rec stop        ->down           --

;; then all four control functions *always* try bring the system into the control's target state
;; meaning that the logic and dependences of how to do that are taken care of

;; then for each status we keep a meta-status, i.e. {:state :stopped :state-status :success} or {:state :running :meta-status :fail :reason ...}

(defn status [system component-id & keys]
  (get-in system (concat (flatten [:components component-id :status]) keys)))
(defn up? [system component-id]
  (= :up (status system component-id)))
(defn running? [system component-id]
  (= :running (status system component-id)))
(defn stopped? [system component-id]
  (= :stopped (status system component-id)))
(defn down? [system component-id]
  (= :down (status system component-id)))
(defn failed? [system component-id]
  (= :failed (status system component-id)))

(defn set-status [system component-id status]
  (assert (#{:up :down :running :stopped} status))
  (log system component-id "Status now" status)
  (assoc-in system (flatten [:components component-id :status]) status))

(defn get-cfg [system component-id & keys]
  (get-in system (concat (flatten [component-id]) keys)))
(defn set-cfg [system component-id cfg]
  (let [new-system (update-in system (flatten [component-id]) 
                              deep-merge 
                              cfg)]
    (log system component-id "Config now" (get-cfg new-system component-id))
    new-system))

(defn configure [system component-id cfg]
  (assoc-in  system (flatten [component-id]) 
             cfg))


(defn component 
  ([id] (component @+system+ id))
  ([system id]
     (get-in system (flatten [:components id :instance]))))

(defn stop-component [system component-id]
  (cond (failed? system component-id)
        (throw (Exception. (str "Cannot stop failed component " component-id)))
        (running? system component-id)
        (let [dependencies (dependencies system component-id)
              system (loop [s system 
                            d dependencies]
                       (if (empty? d)
                         s
                         (recur (stop-component s (first d))
                                (rest d))))]
          (log system component-id "Stopping")
          (if (every? (partial stopped? system) dependencies)
            (if (= :system component-id) ;; avoid endless loops (stop on system calls stop-component)
              (set-status system component-id :stopped)
              (try (set-status 
                    (stop (component system component-id) system)
                    component-id
                    :stopped)
                   (catch Exception e
                     (error system component-id e)
                     (set-status system :failed))))
            (set-status system :failed)))
        :else (set-status system component-id :stopped)))

(defn shutdown-component [system component-id]
  (cond (failed? system component-id)
        (throw (Exception. (str "Cannot shutdown failed component " component-id)))
        (down? system component-id)
        system
        (or (up? system component-id) (stopped? system component-id))
        (let [dependencies (dependencies system component-id)
              system (loop [s system 
                            d dependencies]
                       (if (empty? d)
                         s
                         (recur (shutdown-component s (first d))
                                (rest d))))]
          (log system component-id "Shutting down")
          (if (every? (partial down? system) dependencies)
            (if (= :system component-id) ;; avoid endless loops (shutdown on system calls shutdown-component)
              (set-status system component-id :down)
              (try (set-status 
                    (shutdown (component system component-id) system)
                    component-id
                    :down)
                   (catch Exception e
                     (error system component-id e)
                     (set-status system :component-id :failed))))
            (set-status system :failed)))
        :else ;; case runnning
        (recur (stop (component system component-id) system) component-id)))

(defn initialize-component [system component-id]
  (cond (failed? system component-id)
        (throw (Exception. (str "Cannot shutdown failed component " component-id)))
        (up? system component-id)
        system
        (or (running? system component-id)
            (stopped? system component-id))
        (recur (shutdown (component system component-id) system) component-id)
        :else ;; case down
        (let [dependencies (dependencies system component-id)
              system (loop [s system 
                            d dependencies]
                       (if (empty? d)
                         s
                         (recur (initialize-component s (first d))
                                (rest d))))]
          (log system component-id "Initializing")
          (if (every? (partial up? system) dependencies)
            (if (= :system component-id) ;; avoid endless loops (initialize on system calls initialize-component)
              (set-status system component-id :up)
              (try (set-status 
                    (initialize (component system component-id) system)
                    component-id
                    :up)
                   (catch Exception e
                     (error system component-id e)
                     (set-status system :component-id :failed))))
            (set-status system :failed)))))

(defn start-component [system component-id]
  (cond (failed? system component-id)
        (throw (Exception. (str "Cannot start failed component " component-id)))
        (running? system component-id)
        system
        (or (stopped? system component-id)
            (up? system component-id))
        (let [dependencies (dependencies system component-id)
              system (loop [s system 
                            d dependencies]
                       (if (empty? d)
                         s
                         (recur (start-component s (first d))
                                (rest d))))]
          (log system component-id "Starting")
          (if (every? (partial running? system) dependencies)
            (if (= :system component-id) ;; avoid endless loops (start on system calls start-component)
              (set-status system component-id :running)
              (try (set-status 
                    (start (component system component-id) system)
                    component-id
                    :running)
                   (catch Exception e
                     (error system component-id e)
                     (set-status system :component-id :failed))))
            (set-status system :failed)))
        (down? system component-id)
        (start-component (initialize-component system component-id) component-id)
        :else ;; case down
        (recur (initialize (component system component-id) system) component-id)))

(defn init-system [system id]
  (let [error-channel (as/chan)
        log-channel (as/chan)]
    (as/go-loop []
      (when-let [e (as/<! error-channel)]
        (println "[ERROR]" e)
        (flush)
        (recur)))
    (as/go-loop []
      (when-let [m (as/<! log-channel)]
        (println "[LOG]" m)
        (flush)
        (recur)))
    (initialize-component 
     (set-system-log-channel 
      (set-system-error-channel system error-channel)
      log-channel) 
     id)))

(defcomponent :system []
  :init init-system
  :start (fn [system id] (start-component system id))
  :stop (fn [system id] (stop-component system id))
  :shutdown (fn [system id] 
              (let [new-system (shutdown-component system id)]
                (as/close! (system-error-channel new-system))
                (as/close! (system-log-channel new-system))
                (set-system-error-channel system nil)
                (set-system-log-channel system nil))))

