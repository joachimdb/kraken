(ns kraken.model
  (:require [clj-time.format :as tformat]
            [clj-time.coerce :as tcoerce]
            [clj-time.core :as tcore]
            [clojure.core.async :as as]
            [taoensso.timbre :as timbre]))

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

(defn system-log-channel [s]
  (:log-channel s))
(defn set-system-log-channel [s lc]
  (assoc s :log-channel lc))
(defn log [system component-id level msg]
  (if-let [log-channel (system-log-channel system)]
    (as/put! log-channel {:source component-id
                          :level level
                          :msg msg
                          :time (tcore/now)})
    (throw (Exception. "Log failed: System not initialized"))))
(defn info [system component-id msg]
  (log system component-id :info msg))
(defn error [system component-id exception msg]
  (log system component-id :error {:exception exception :msg msg}))

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

(defn set-status [system component-id target]
  (assert (#{:up :down :running :stopped :failed} target))
  (info system component-id (str "System " (vec (flatten [component-id])) " - [" (status system component-id) " => " target "]"))
  (assoc-in system (flatten [:components component-id :status]) target))

(defn get-cfg [system component-id & keys]
  (get-in system (concat (flatten [component-id]) keys)))
(defn set-cfg [system component-id cfg]
  (let [new-system (update-in system (flatten [component-id]) 
                              deep-merge 
                              cfg)]
    (info system component-id (str "System " (vec (flatten [component-id])) " - set-cfg  " (get-cfg new-system component-id)))
    new-system))

(defn configure [system component-id cfg]
  (assoc-in  system (flatten [component-id]) 
             cfg))

(defn component 
  ([id] (component @+system+ id))
  ([system id]
     (get-in system (flatten [:components id :instance]))))

(def targets (atom {}))

(defn target-context [target]
  (get-in @targets [target :context]))
(defn target-component-handler [target]
  (get-in @targets [target :component-handler]))

(declare switch-context)

(defn handle-dependencies [system component-id target-status]
  (let [dependencies (dependencies system component-id)
        system (reduce #(switch-context %1 %2 target-status) system dependencies)]
    (if (and (not (failed? system component-id))
             (every? #(= (status system %) target-status) dependencies))
      (set-status system component-id target-status)
      (set-status system component-id :failed))))

(defn switch-context [system component-id target]
  (cond (= (status system component-id) :failed)
        (throw (Exception. "Cannot switch from failed context"))
        (= (status system component-id) target) ;; nothing to do
        system
        ((target-context target) (status system component-id)) ;; can call handler in current context 
        (handle-dependencies (try ((target-component-handler target) (component system component-id) system)
                                  (catch Exception e
                                    (error system component-id e (str "System " (vec (flatten [component-id])) " - [" (status system get-in system component-id) " => " target "]"))
                                    (set-status system component-id :failed)))
                             component-id
                             target)
        :else ;; not a valid context, try calling a handler that achieves one and recur
        (loop [subtargets (target-context target)
               system system]
          (if (empty? subtargets)
            system ;; failed to switch context
            (let [new-system (switch-context system component-id (first subtargets))]
              (if (= (first subtargets) (status new-system component-id))
                (switch-context new-system component-id target)
                (recur (rest subtargets) new-system)))))))

(defmacro deftarget [target & {:keys [context component-handler]
                               :as keys}]
  `(swap! targets assoc ~target ~keys))


(deftarget :stopped
  :context #{:running :up}
  :component-handler stop)
(deftarget :down 
  :context #{:stopped :up}
  :component-handler shutdown)
(deftarget :up
  :context #{:down}
  :component-handler initialize)
(deftarget :running
  :context #{:up :stopped}
  :component-handler start)

(defn stop-system [system id]
  (assert (= :system id))
  (log system id :info "Stopping main system")
  (handle-dependencies system id :stopped))

(defn start-system [system id]
  (assert (= :system id))
  (log system id :info "Starting main system")
  (handle-dependencies system id :running))

(defn init-system [system id]
  (assert (= :system id))
  (let [log-channel (as/chan)
        system (set-system-log-channel system log-channel)]
    (as/go-loop []
      (if-let [m (as/<! log-channel)]
        (do (if-let [e (get-in m [:msg :exception])]
              (timbre/log (:level m) e (:msg m))
              (timbre/log (:level m) (:msg m)))
            (recur))
        (as/close! log-channel)))
    (log system id :info "Initializing main system")
    (handle-dependencies system id :up)))

(defn shutdown-system [system id]
  (assert (= :system id))
  (log system id :info "Shutting down main system")
  (let [system (handle-dependencies system id :down)]
    (as/close! (system-log-channel system))
    (set-system-log-channel system nil)))

(defcomponent :system []
  :init init-system
  :start start-system
  :stop stop-system
  :shutdown shutdown-system)

