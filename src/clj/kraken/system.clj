(ns kraken.system
  (:use [clojure.walk])
  (:require [clj-time.format :as tformat]
            [clj-time.coerce :as tcoerce]
            [clj-time.core :as tcore]
            [clojure.core.async :as as]
            [taoensso.timbre :as timbre]))

;;; TODO
;;; - systems should receive their config as specified in external config file at the start of initialization
;;; - handle exceptions when a component fails to switch context

(defn deep-merge
  "Recursively merges maps. If keys are not maps, the last value wins."
  [& vals]
  (if (every? map? vals)
    (apply merge-with deep-merge vals)
    (last vals)))

(defprotocol ComponentP
  (initial-config [this])
  (initialize [this system])
  (start [this system])
  (stop [this system])
  (shutdown [this system]))

(def +system+ (atom {}))

(defn system []
  @+system+)

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
    (do (timbre/warn "Log failed: System not properly initialized?")
        (timbre/log level msg)))
  system)

(defn info [system component-id msg]
  (log system component-id :info msg))
(defn warn 
  ([system component-id msg] 
     (log system component-id :warn msg))
  ([system component-id exception msg]
     (log system component-id :warn {:exception exception :msg msg})))
(defn error 
  ([system component-id msg] 
     (log system component-id :error msg))
  ([system component-id exception msg]
     (log system component-id :error {:exception exception :msg msg})))

(defn status [system component-id & keys]
  (get-in system (concat (flatten [:components component-id :status]) keys)))
(defn up? 
  ([system] (up? system :main-system))
  ([system component-id]
     (= :up (status system component-id))))
(defn running? 
  ([system] (running? system :main-system))
  ([system component-id]
     (= :running (status system component-id))))
(defn stopped? 
  ([system] (stopped? system :main-system))
  ([system component-id]
     (= :stopped (status system component-id))))
(defn down? 
  ([system] (down? system :main-system))
  ([system component-id]
     (= :down (status system component-id))))
(defn failed? 
  ([system] (failed? system :main-system))
  ([system component-id]
     (= :failed (status system component-id))))

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
(defn mk-component
  ([id] (mk-component @+system+ id))
  ([system id]
     (if-let [factory (get-in system (flatten [:components id :factory]))]
       (do (timbre/info "Making component" (vec (flatten [id])))
           (factory)))
     (throw (Exception. (str "No factory found for component " (vec (flatten [id])) ". System not properly initialized?")))))

(def targets (atom {}))

(defn target-context [target]
  (get-in @targets [target :context]))

(defn target-component-handler [target]
  (get-in @targets [target :component-handler]))

(declare switch-context)

(defn switch-dependencies 
  ([system component-id target-status] (switch-dependencies system component-id target-status #{}))
  ([system component-id target-status skip-components]
     (let [dependencies (dependencies system component-id)
           system (reduce #(switch-context %1 %2 target-status) system (remove skip-components dependencies))]
       (if (and (not (failed? system component-id))
                (every? #(= (status system %) target-status) dependencies))
         (set-status system component-id target-status)
         (set-status system component-id :failed)))))

(defn switch-component [system component-id target]
  (try ((target-component-handler target) (component system component-id) system)
       (catch Exception e
         (error system component-id e (str "System " (vec (flatten [component-id])) " - [" (status system get-in system component-id) " => " target "]"))
         (set-status system component-id :failed))))

(defn switch-context 
  ([system component-id target] (switch-context system component-id target #{} #{}))
  ([system component-id target skip-components skip-targets]
     (let [system (if (= :up target)
                    (let [cfg (try (initial-config (component system component-id))
                                   (catch Exception e 
                                     (warn system component-id e (str "System " (vec (flatten [component-id])) " - Could not retrieve initial configuration, proceeding with empty config."))
                                     {}))]
                      (configure system component-id cfg))
                    system)]
       (cond (= (status system component-id) target) ;; nothing to do
             system
             ((target-context target) (status system component-id)) ;; can call handler in current context 
             (switch-dependencies (if (skip-components component-id)
                                    system
                                    (switch-component system component-id target))
                                  component-id
                                  target
                                  (conj skip-components component-id))
             :else ;; not a valid context, try calling a handler that achieves one and recur
             (loop [subtargets (remove skip-targets (target-context target))
                    system system]
               (if (empty? subtargets)
                 (warn system component-id (str "Failed to switch to context" target)) 
                 (let [new-system (switch-context system component-id (first subtargets) skip-components (conj skip-targets target))]
                   (if (= (first subtargets) (status new-system component-id))
                     (switch-context new-system component-id target skip-components skip-targets)
                     (recur (rest subtargets) new-system)))))))))

(defmacro deftarget [target & {:keys [context component-handler]
                               :as keys}]
  `(swap! targets assoc ~target ~keys))

(deftarget :stopped
  :context #{:running}
  :component-handler stop)
(deftarget :down 
  :context #{:stopped :up}
  :component-handler shutdown)
(deftarget :up
  :context #{:down :failed}
  :component-handler initialize)
(deftarget :running
  :context #{:up :stopped}
  :component-handler start)
(deftarget :failed
  :context #{}
  :component-handler nil)

(defn init-system 
  ([system] (init-system system :main-system))
  ([system component-id]
     (timbre/info "Initializing system" component-id)
     (let [system (if (= :main-system component-id)
                    (let [log-channel (as/chan)
                          system (set-system-log-channel system log-channel)]
                      (as/go-loop []
                        (if-let [m (as/<! log-channel)]
                          (do (if-let [e (get-in m [:msg :exception])]
                                (timbre/log (:level m) e (:msg m))
                                (timbre/log (:level m) (:msg m)))
                              (recur))
                          (as/close! log-channel)))
                      system)
                    system)]
       (switch-context system component-id :up #{component-id}  #{}))))

(defn stop-system 
  ([system] (stop-system system :main-system))
  ([system component-id]
     (timbre/info "Stopping system" component-id) ;; can't use info at this level (system may not be initialized)     
     (switch-context (if (down? system)
                       (init-system system)
                       system)
                     component-id :stopped #{component-id} #{})))

(defn start-system 
  ([system] (start-system system :main-system))
  ([system component-id]
     (timbre/info "Starting system" component-id)
     (switch-context (if (down? system)
                       (init-system system)
                       system)
                     component-id :running #{component-id}  #{})))

(defn shutdown-system 
  ([system] (shutdown-system system :main-system))
  ([system component-id]
     (timbre/info "Shutting down system" component-id)
     (let [system (switch-context system component-id :down #{component-id} #{})]
       (if (= :main-system component-id)
         (do (when-let [log-chan (system-log-channel system)]
               (as/close! log-chan))
             (set-system-log-channel system nil))
         system))))

(defn camelize [path] 
  (let [words (clojure.string/split (name path) #"[\s_-]+")] 
    (clojure.string/join "" (cons (clojure.string/lower-case (first words)) 
                                  (map clojure.string/capitalize (rest words))))))

(defmacro defcomponent [id [:as dependencies] & clauses]
  `(do (when-let [prev# (component @+system+ ~id)]
         (when-not (down? @+system+ ~id)
           (timbre/warn (str "System " ~(vec (flatten [id])) " - [" (status @+system+ ~id)  " => :down]"))
           (shutdown prev# @+system+)))
       (deftype ~(symbol (camelize (clojure.string/replace (apply str (flatten [id])) #":" "-"))) [] ;; ~slots
         ~@clauses)
       (defn ~(symbol (clojure.string/replace (apply str (flatten ["mk" id])) #":" "-")) [] ;; ~slots
         (~(symbol (camelize (clojure.string/replace (apply str (flatten [id "."])) #":" "-")))
          ;; ~@slots
          ))
       (timbre/info "New component" ~id "- dependencies" ~dependencies)
       (when-not (or (= :main-system ~id)
                     (down? @+system+ :main-system))
         (timbre/warn (str "System " [:main-system] " - [" (status @+system+ :main-system) " => :down]")))
       (swap! +system+
              #(assoc-in 
                (update-in
                 (deep-merge %
                             (assoc-in {}
                                       (flatten [:components ~id]) 
                                       {:instance (~(symbol (clojure.string/replace (apply str (flatten ["mk" id])) #":" "-")) ;; ~@(map (constantly nil) slots)
                                                   )
                                        :factory ~(symbol (clojure.string/replace (apply str (flatten ["mk" id])) #":" "-"))
                                        :status :down}))
                 [:components :main-system :dependencies]
                 clojure.set/union 
                 ~(disj (conj (into #{} dependencies) id) :main-system))
                [:components :main-system :status]
                :down))))

(defcomponent :main-system []  
  ComponentP 
  (initial-config [this] {:system {:started (tcore/now)}})
  (initialize [this system] (init-system system))
  (start [this system] (start-system system))
  (stop [this system] (stop-system system))
  (shutdown [this system] (shutdown-system system)))

(defn configure! [component-id cfg]
  (swap! +system+ #(set-cfg % component-id cfg)))
(defn configuration [component-id & keys]
  (apply get-cfg @+system+ component-id keys))
(defn initialize!
  ([] (initialize! :main-system))
  ([component-id]
     (swap! +system+ #(initialize (component % component-id) %))))
(defn start!
  ([] (start! :main-system))
  ([component-id]
     (swap! +system+ #(start (component % component-id) %))))
(defn stop! 
  ([] (stop! :main-system))
  ([component-id]
     (swap! +system+ #(stop (component % component-id) %))))
(defn shutdown! 
  ([] (shutdown! :main-system))
  ([component-id]
     (swap! +system+ #(shutdown (component % component-id) %))))

(defn dependency-seq
  ([system] (dependency-seq system :main-system))
  ([system from] (dependency-seq system :main-system #{}))
  ([system from seen]
     (let [deps (dependencies system from)
           seen (into seen deps)]
       (conj (map #(dependency-seq system % seen) 
                  deps)
             from))))

(defn hard-reset! []
  (shutdown!)
  (swap! +system+ deep-merge
         {:components 
          (apply hash-map
                 (flatten 
                  (postwalk (fn [id]
                              (if (seq? id)
                                id
                                [id {:instance (mk-component id)
                                     :status :down}]))
                            (dependency-seq @+system+))))}))

