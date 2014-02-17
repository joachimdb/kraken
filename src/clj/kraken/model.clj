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
  (if-let [error-channel (system-error-channel system)]
    (as/put! error-channel {:source component-id
                            :error rest
                            :time (tcore/now)})
    (throw (Exception. "Error log failed: System not initialized"))))
  
(defn system-log-channel [s]
  (:log-channel s))
(defn set-system-log-channel [s lc]
  (assoc s :log-channel lc))
(defn log [system component-id & rest]
  (if-let [log-channel (system-log-channel system)]
    (as/put! log-channel {:source component-id
                          :message rest
                          :time (tcore/now)})
    (throw (Exception. "Log failed: System not initialized"))))

;; TODO: cleanup status mess and work out order of things:
;;               up        running          stopped        down
;; initialize    --      rec shutdown    rec shutdown      ->up
;; start     ->running       --          ->runnning      rec initialize
;; stop      ->stopped    ->stopped           --         rec initialize
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

(defn percolate [system action component-ids]
  (loop [s system 
         ids component-ids]
    (if (empty? ids)
      s
      (recur (action s (first ids))
             (rest ids)))))

;; (defn stop-component [system component-id]
;;   (assert (not (= :system component-id)))
;;   (cond (failed? system component-id)
;;         (throw (Exception. (str "Cannot stop failed component " component-id)))
;;         (or (running? system component-id) (up? system component-id))
;;         (let [dependencies (dependencies system component-id)
;;               system (percolate system stop-component dependencies)]
;;           (log system component-id "Stopping")
;;           (if (every? (partial stopped? system) dependencies)
;;             (try (set-status 
;;                   (stop (component system component-id) system)
;;                   component-id
;;                   :stopped)
;;                  (catch Exception e
;;                    (error system component-id e)
;;                    (set-status system :failed)))
;;             (set-status system component-id :failed)))
;;         :else ;; case down
;;         (recur (initialize (component system component-id) system) component-id)))

;; (defn shutdown-component [system component-id]
;;   (assert (not (= :system component-id)))
;;   (cond (failed? system component-id)
;;         (throw (Exception. (str "Cannot shutdown failed component " component-id)))
;;         (down? system component-id)
;;         system
;;         (or (up? system component-id) (stopped? system component-id))
;;         (let [dependencies (dependencies system component-id)
;;               system (percolate system shutdown-component dependencies)]
;;           (log system component-id "Shutting down")
;;           (if (every? (partial down? system) dependencies)
;;             (try (set-status 
;;                   (shutdown (component system component-id) system)
;;                   component-id
;;                   :down)
;;                  (catch Exception e
;;                    (error system component-id e)
;;                    (set-status system :component-id :failed)))
;;             (set-status system component-id :failed)))
;;         :else ;; case runnning
;;         (recur (stop (component system component-id) system) component-id)))

;; (defn initialize-component [system component-id]
;;   (assert (not (= :system component-id)))
;;   (cond (failed? system component-id)
;;         (throw (Exception. (str "Cannot shutdown failed component " component-id)))
;;         (up? system component-id)
;;         system
;;         (or (running? system component-id)
;;             (stopped? system component-id))
;;         (recur (shutdown (component system component-id) system) component-id)
;;         :else ;; case down
;;         (let [dependencies (dependencies system component-id)
;;               system (percolate system initialize-component dependencies)]
;;           (log system component-id "Initializing")
;;           (if (every? (partial up? system) dependencies)
;;             (try (set-status 
;;                   (initialize (component system component-id) system)
;;                   component-id
;;                   :up)
;;                  (catch Exception e
;;                    (error system component-id e)
;;                    (set-status system :component-id :failed)))
;;             (set-status system component-id :failed)))))

;; (defn start-component [system component-id]
;;   (assert (not (= :system component-id)))
;;   (cond (failed? system component-id)
;;         (throw (Exception. (str "Cannot start failed component " component-id)))
;;         (running? system component-id)
;;         system
;;         (or (stopped? system component-id)
;;             (up? system component-id))
;;         (let [dependencies (dependencies system component-id)
;;               system (percolate system start-component dependencies)]
;;           (log system component-id "Starting")
;;           (if (every? (partial running? system) dependencies)
;;             (try (set-status 
;;                   (start (component system component-id) system)
;;                   component-id
;;                   :running)
;;                  (catch Exception e
;;                    (error system component-id e)
;;                    (set-status system :component-id :failed)))
;;             (set-status system component-id :failed)))
;;         (down? system component-id)
;;         (start-component (initialize-component system component-id) component-id)
;;         :else ;; case down
;;         (recur (initialize (component system component-id) system) component-id)))

(defn handle-dependencies [system component-id action result-status]
  (let [dependencies (dependencies system component-id)
        system (percolate system action dependencies)]
    (if (and (not (failed? system component-id))
             (every? #(= (status system component-id) result-status) dependencies))
      (set-status system component-id result-status)
      (set-status system component-id :failed))))

(defmacro defcontrol [name [system component-id result-status] & [:as clauses]]
  `(defn ~name [~system ~component-id]
     (assert (not (= :system ~component-id)))
     (log ~system ~component-id (str "Received control " ~name))
     (cond (= (status ~system ~component-id) ~result-status)
           ~system
           (failed? ~system ~component-id)
           (throw (Exception. (str "Cannot control failed component " ~component-id)))
           ~@clauses)))

;; (def targets (atom {}))

;; (defn target-context [target]
;;   (get-in @targets [target :context]))
;; (defn target-handler [target]
;;   (get-in @targets [target :handler]))

;; (defn switch-context [system component-id target]
;;   (cond (= (status system component-id) target) ;; nothing to do
;;         system
;;         ((target-context target) (status system component-id)) ;; can call handler in current context
;;         ((target-handler target) system component-id)
;;         :else ;; not a valid context, try calling a handler that achieves one and recur
;;         (loop [subtargets (target-context target)
;;                system system]
;;           (if (empty? subtargets)
;;             system ;; failed to switch context
;;             (let [new-system (switch-context system component-id (first subtargets))]
;;               (if (= (first subtargets) (status new-system component-id))
;;                 (switch-context new-system component-id target)
;;                 (recur (rest subtargets) new-system)))))))

;; (defn handle-dependencies [system component-id target]
;;   (let [dependencies (dependencies system component-id)
;;         system (loop [s system 
;;                       ids dependencies]
;;                  (if (empty? ids)
;;                    s
;;                    (recur (switch-context s (first ids) target)
;;                           (rest ids))))]
;;     (if (and (not (failed? system component-id))
;;              (every? #(= (status system %) target-status)   dependencies))
      
;;       (set-status system component-id result-status)
;;       (set-status system component-id :failed))))


;; (defmacro deftarget [target & [system component-id] {:keys [context handler]
;;                                                      :as params} ]
;;   (swap! targets assoc target 
;;          {:context context
;;           :handler (fn [~system ~component-id]
;;                      (handle-dependencies (try (~handler (component ~system ~component-id) ~system)
;;                                                (catch Exception e
;;                                                  (error ~system ~component-id e)
;;                                                  (set-status ~system :failed)))
;;                                           ~component-id stop-component :stopped))}))

;; (deftarget :stopped
;;   :context #{:running :up}
;;   :handler stop)
;; (deftarget :down [system component-id]
;;   :context #{:stopped :up}
;;   :handler stop)


;;; state, action that achieves state, states on which action can be performed

(defcontrol stop-component [system component-id :stopped]
  (or (running? system component-id) (up? system component-id))
  (handle-dependencies (try (stop (component system component-id) system)
                            (catch Exception e
                              (error system component-id e)
                              (set-status system :failed)))
                       component-id stop-component :stopped)
  :else ;; case down
  (recur (initialize (component system component-id) system) component-id))

(defcontrol shutdown-component [system component-id :down]
  (or (up? system component-id) (stopped? system component-id))
  (handle-dependencies (try (shutdown (component system component-id) system)
                            (catch Exception e
                              (error system component-id e)
                              (set-status system :failed)))
                       component-id shutdown-component :down)
  :else ;; case runnning
  (recur (stop (component system component-id) system) component-id))

(defcontrol initialize-component [system component-id :up]
  (or (running? system component-id)
      (stopped? system component-id))
  (recur (shutdown (component system component-id) system) component-id)
  :else ;; case down
  (handle-dependencies (try (initialize (component system component-id) system)
                            (catch Exception e
                              (error system component-id e)
                              (set-status system :failed)))
                       component-id shutdown-component :up))

(defcontrol start-component [system component-id :running]
  (or (stopped? system component-id)
      (up? system component-id))
  (handle-dependencies (try (start (component system component-id) system)
                            (catch Exception e
                              (error system component-id e)
                              (set-status system :failed)))
                       component-id shutdown-component :running)
  (down? system component-id)
  (start-component (initialize-component system component-id) component-id)
  :else ;; case down
  (recur (initialize (component system component-id) system) component-id))

(defn stop-system [system id]
  (assert (= :system id))
  (let [dependencies (dependencies system id)]
    (let [new-system (percolate system stop-component dependencies)]
      (log new-system id "Stopping")
      (if (every? (partial stopped? new-system) dependencies)
        (set-status new-system id :stopped)
        (set-status new-system id :failed)))))

(defn start-system [system id]
  (assert (= :system id))
  (let [dependencies (dependencies system id)]
    (let [new-system (percolate system start-component dependencies)]
      (log new-system id "Starting")
      (if (every? (partial stopped? new-system) dependencies)
        (set-status new-system id :running)
        (set-status new-system id :failed)))))

(defn init-system [system id]
  (assert (= :system id))
  (let [error-channel (as/chan)
        log-channel (as/chan)
        dependencies (dependencies system id)]
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
    (let [new-system (percolate (set-system-log-channel 
                                 (set-system-error-channel system error-channel)
                                 log-channel)
                                initialize-component 
                                dependencies)]
      (log new-system id "Initializing")
      (if (every? (partial up? new-system) dependencies)
        (set-status new-system id :up)
        (set-status new-system id :failed)))))

(defn shutdown-system [system id]
  (assert (= :system id))
  (let [dependencies (dependencies system id)]
    (let [new-system (#(if (every? (partial down? %) dependencies)
                         (set-status % id :down)
                         (set-status % id :failed))
                      (percolate system shutdown-component dependencies))]
      (log new-system id "Shutting down")
      (as/close! (system-error-channel new-system))
      (as/close! (system-log-channel new-system))
      (set-system-error-channel 
       (set-system-log-channel new-system nil) 
       nil))))

(defcomponent :system []
  :init init-system
  :start start-system
  :stop stop-system
  :shutdown shutdown-system)

