(ns kraken.channels
  (:require [clojure.core.async :as as]
            [clj-time.core :as tcore]
            [clj-time.coerce :as tcoerce]))


(defprotocol ConnectionP
  (disconnect! [this]))

(defrecord Connection [source sink channel]
  ConnectionP
  (disconnect! [this] 
    (as/untap source channel)
    (as/unmix sink channel)
    (as/close! channel)
    nil))

(defn connect! 
  "Creates and returns a channel connecting a source and a sink."
  ([source sink] (connect! source sink (as/chan)))
  ([source sink chan]
     (as/tap source chan)
     (as/admix sink chan)
     (Connection. source sink chan)))

;;; I. General channel creation functions
;;; =====================================

(defn repeat-channel 
  "Creates and returns a channel that is filled with x."
  ([x] (repeat-channel nil x))
  ([n x]
     (let [out (as/chan 1)]
       (as/go-loop [i 0]
                   (if (or (nil? n) (> n i))
                     (do (as/>! out x)
                         (recur (inc i)))
                     (as/close! out)))
       out)))

;; (defn repeatedly-channel 
;;   "Creates and returns a channel that is filled with repeated calls to f. Default time interval
;; between calls is 1000ms. The channel is closed when f returns nil or when f throws an exception
;; and on-fail is nil (default). Parks if no buffer space available. "
;;   ([f] (repeatedly-channel nil 1000 f nil))
;;   ([f on-fail] (repeatedly-channel nil 1000 f on-fail))
;;   ([msecs f on-fail] (repeatedly-channel nil msecs f on-fail))
;;   ([n msecs f on-fail]
;;      (let [out (as/chan 1)]
;;        (as/go-loop [i 0]
;;          (if (or (nil? n) (> n i))
;;            (let [v (try (f) (catch Exception e on-fail))]
;;              (if (nil? v)
;;                (as/close! out)
;;                (do ;; (println "rep sending" v)
;;                  (as/>! out v)
;;                  (when msecs 
;;                    (as/<! (as/timeout msecs)))
;;                  (recur (inc i)))))
;;            (as/close! out)))
;;        out)))

(defn repeatedly-channel 
  "Creates and returns a channel that is filled with repeated calls to f.  The channel is closed
when f returns nil or throws an exception. Parks if no buffer space available. "
  ([f] (repeatedly-channel nil f))
  ([n f]
     (let [out (as/chan)]
       (as/go-loop [i 0]
         (if (or (nil? n) (> n i))
           (let [v (try (f) (catch Exception e nil))]
             (if (nil? v)
               (as/close! out)
               (do (as/>! out v)
                   (recur (inc i)))))
           (as/close! out)))
       out)))

(defn polling-channel
  "Creates and returns a channel and puts the result of calls to f onto it. The time between
successive calls can be controlled with the interval in msecs. The channel is closed when the call
to f returns nil. What happens if f throws an exception depends on the value of on-fail. If it is
nil, the chanel is closed, otherwise on-fail is sent through the channel. Parks if no buffer space
is available."
  ([f & {:keys [interval on-fail]
         :or {interval 1000, on-fail nil}}]
     (let [out (as/chan)]
       (as/go-loop []
         (let [v (try (f) (catch Exception e on-fail))]
           ;; (println "poll result: " v)
           (if (nil? v)
             (as/close! out)
             (do ;; (println "Putting...")
                 (as/>! out v)
                 ;; (println "Pausing poll...")
                 (as/<! (as/timeout interval))
                 (recur)))))
       out)))

;; (defn pulse 
;;   "Creates and returns a channel that outputs true every msecs milliseconds."
;;   ([msecs] (pulse nil msecs))
;;   ([n msecs]
;;      (repeatedly-channel n msecs (constantly true) nil)
;;      (let [out (as/chan)]
;;        (as/go-loop [i 0]
;;                    (if (or (nil? n) (> n i))
;;                      (do (as/put! out true)
;;                          (as/<! (as/timeout msecs))
;;                          (recur (inc i)))
;;                      (as/close! out)))
;;        out)))

;; (defn clock 
;;   "Creates and returns a channel that outputs a time in msecs (a long) every msecs milliseconds."
;;   ([msecs] (pulse nil msecs))
;;   ([n msecs]
;;      (repeatedly-channel n msecs tcore/now nil)
;;      (let [out (as/chan)]
;;        (as/go-loop [i 0]
;;                    (if (or (nil? n) (> n i))
;;                      (do (as/put! out true)
;;                          (as/<! (as/timeout msecs))
;;                          (recur (inc i)))
;;                      (as/close! out)))
;;        out)))




;; (defn sinusoidal-channel [& {:keys [period amplitude]
;;                              :or {period 1000
;;                                   signal-amplitude 1.0}}]
;;   (as/map< #(* amplitude (Math/sin (/ % period)))
;;            (clock)))

;;; II. General channel transformations
;;; ===================================

(defn mmap
  "Takes a function and a collection of source channels, and returns a
  channel which contains the values produced by applying f to the set
  of first items taken from each source channel, followed by applying
  f to the set of second items from each channel, until any one of the
  channels is closed, at which point the output channel will be
  closed. The returned channel will be unbuffered by default, or a
  buf-or-n can be supplied"
  ([f chs] (mmap f chs nil))
  ([f chs buf-or-n]
     (let [chs (vec chs)
           out (as/chan buf-or-n)
           cnt (count chs)
           rets (object-array cnt)
           dchan (as/chan 1)
           dctr (atom nil)
           done (mapv (fn [i]
                        (fn [ret]
                          (aset rets i ret)
                          (when (zero? (swap! dctr dec))
                            (as/put! dchan (java.util.Arrays/copyOf rets cnt)))))
                      (range cnt))]
       (as/go-loop []
         (reset! dctr cnt)
         (dotimes [i cnt]
           (try
             (as/take! (chs i) (done i))
             (catch Exception e
               (swap! dctr dec))))
         (let [rets (as/<! dchan)]
           (if (some nil? rets)
             (as/close! out)
             (let [v (apply f rets)]
               (as/put! out v)
               (recur)))))
       out)))

(defn ewa-channel
  "Creates and returns a channel that outputs the exponentially weighted average of input
channel. Closes when input channel is closed or when the average cannot be computed."
  [channel alpha & {:keys [key] :or {key identity}}]
  (let [prev (atom (key (as/<!! channel)))        
        alpha* (- 1.0 alpha)]
    (as/map< (fn [new] (let [new (key new)
                             ewa (+ (* alpha @prev) (* alpha* new))]
                         (reset! prev ewa)))
             channel)))

(defn tewa-channel
  "Creates and returns a channel that outputs the exponentially weighted average of input
channel. This is like an ewa channel where the decay parameter alpha is modulatied by a factor
exp(-dt/relaxation). Closes when input channel is closed or when the average cannot be computed. The
relaxtion time is in msecs."
  [channel value-key time-key alpha relaxation]
  (let [initial (key (as/<!! channel))
        prev-val (atom (value-key initial))        
        prev-time (atom (time-key initial))]
    (as/map< (fn [new] (let [new-val (value-key new)
                             new-time (time-key new)
                             dt (- new-time @prev-time)
                             mod-alpha (* alpha (Math/exp (- (/ dt relaxation))))
                             tewa (+ (* mod-alpha @prev-val) (* (- 1.0 mod-alpha) new-val))]
                         (reset! prev-time new-time)
                         (reset! prev-val tewa)))
             channel)))
             

(defn weighted-tewa-channel
  "Creates and returns a channel that outputs the exponentially weighted average of input
channel. This is like a tewa channel where the modulation parameter akpha is additionally modulated
by a power of weight/tewa(weights). For instance, when calculating the price of an asset based on
trades on a market, high volume trades should weigh more than low volume trades. This is
accomplished by modulating alpha with (volume/avg-volume)^beta." 
  [channel value-key time-key weight-key alpha relaxation beta]
  (let [initial (key (as/<!! channel))
        prev-val (atom (value-key initial))        
        prev-time (atom (time-key initial))]
    (as/map< (fn [new ewa-weight]
               (let [new-val (value-key new)
                     new-time (time-key new)
                     dt (- new-time @prev-time)
                     alpha (min 1.0 (max 0.0 (* alpha (Math/pow (/ ewa-weight new-val) beta))))
                     mod-alpha (* alpha (Math/exp (- (/ dt relaxation))))
                     wtewa (+ (* mod-alpha @prev-val) (* (- 1.0 mod-alpha) new-val))]
                 (reset! prev-time new-time)
                 (reset! prev-val wtewa)))
             channel)))
             

(defn diff-channel
  "Creates and returns a channel that outputs the derivative of input channel. Closes when input
channel is closed or when on-fail is nill and the input channel is not differentiable."
  ([channel value-key time-key on-fail]
     (let [ch (as/chan)]
       (as/go (loop [prev (as/<! channel)]
                (if (nil? prev)
                  (as/close! ch)
                  (if-let [next (as/<! channel)]
                    (let [dx (double (- (value-key next) (value-key @prev)))
                          dt (double (- (time-key next) (time-key prev)))
                          val (try (/ dx dt) (catch Exception e on-fail))]
                      (if (nil? dt) 
                        (as/close! ch) 
                        (do (as/put! ch val)
                            (recur next))))
                    (as/close! ch)))))
       ch)))

(defn integrated-channel [channel value-key time-key]
  (let [ch (as/chan)]
    (as/go-loop [prev (as/<! channel)
                 acc 0]
        (if (nil? prev)
          (as/close! ch)
          (if-let [next (as/<! channel)]
            (let [height (/ (+ (value-key next) (value-key prev)) 2) 
                  dt (- (time-key next) (time-key prev))
                  acc (+ acc (* height dt))]
              (as/put! ch acc)
              (recur next acc))
            (as/close! ch))))
    ch))

(defn interleave-channels
  "Returns a channel outputing the first item in each input channel, then the second etc. Closes
when any of the input channels closes."
  ([& channels]
     (as/mapcat< identity
                 (mmap 
                  (fn [& vals] vals)
                  channels))))


;; ;;; III. Sources (mults)
;; ;;; ====================

;; (defn dropping-mult
;;   "Creates and returns a mult(iple) of the supplied channel. Channels
;;   containing copies of the channel can be created with 'tap', and
;;   detached with 'untap'.

;;   Each item is distributed to all taps in parallel and synchronously,
;;   i.e. each tap must accept before the next item is distributed. Use
;;   buffering/windowing to prevent slow taps from holding up the mult.

;;   Items received when there are no taps are dropped.

;;   If a tap throws an exception it is removed from the mult."
;;   [ch] 
;;   (let [cs (atom {}) ;; ch->close?
;;         m (reify
;;             as/Mux
;;             (as/muxch* [_] ch)

;;             as/Mult
;;             (as/tap* [_ ch close?] (swap! cs assoc ch close?) nil)
;;             (as/untap* [_ ch] (swap! cs dissoc ch) nil)
;;             (as/untap-all* [_] (reset! cs {}) nil))
;;         dchan (as/chan 1)
;;         dctr (atom nil)
;;         done #(when (zero? (swap! dctr dec))
;;                 (as/put! dchan true))]
;;     (as/go-loop []
;;                 (let [val (as/<! ch)]
;;                   (if (nil? val)
;;                     (doseq [[c close?] @cs]
;;                       (when close? (as/close! c)))
;;                     (let [chs (keys @cs)]
;;                       (reset! dctr (count chs))
;;                       (doseq [c chs]
;;                         (try
;;                           (as/put! c val done)
;;                           (catch Exception e
;;                             (swap! dctr dec)
;;                             (as/untap* m c))))
;;                       ;;wait for all unless no subscriptions
;;                       (when-not (empty? @cs) (as/<! dchan))
;;                       (recur)))))
;;     m))


;; (defn blocking-mult
;;   "Creates and returns a mult(iple) of the supplied channel. Channels
;;   containing copies of the channel can be created with 'tap', and
;;   detached with 'untap'.

;;   Each item is distributed to all taps in parallel and synchronously,
;;   i.e. each tap must accept before the next item is distributed. Use
;;   buffering/windowing to prevent slow taps from holding up the mult.

;;   Stops polling when no taps are registered

;;   If a tap put throws an exception, it will be removed from the mult."
;;   [ch] 
;;   (let [cs (atom {}) ;; ch->close?
;;         cchan (as/chan)
;;         m (reify
;;             as/Mux
;;             (as/muxch* [_] ch)

;;             as/Mult
;;             (as/tap* [_ ch close?] (swap! cs assoc ch close?) (as/put! cchan true) nil)
;;             (as/untap* [_ ch] (swap! cs dissoc ch) nil)
;;             (as/untap-all* [_] (reset! cs {}) nil))
;;         dchan (as/chan 1)
;;         dctr (atom nil)
;;         done #(when (zero? (swap! dctr dec))
;;                 (as/put! dchan true))]
;;     (as/go-loop []
;;                 (let [val (as/<! ch)]
;;                   (if (nil? val)
;;                     (doseq [[c close?] @cs]
;;                       (when close? (as/close! c)))
;;                     (let [chs (keys @cs)]
;;                       (reset! dctr (count chs))
;;                       (doseq [c chs]
;;                         (try
;;                           (as/put! c val done)
;;                           (catch Exception e
;;                             (swap! dctr dec)
;;                             (as/untap* m c))))                      
;;                       (when (empty? @cs)
;;                         (as/<! cchan)) ;; wait until tapped 
;;                       ;;wait for all 
;;                       (as/<! dchan)
;;                       (recur)))))
;;     m))

;; (defn constant-source 
;;   "Creates and returns a source that constantly outputs x."
;;   ([x] (constant-source nil x))
;;   ([n x]
;;      (blocking-mult (repeat-channel n x))))

;; (defn dynamic-source 
;;   "Creates and returns a source that is filled with repeated calls to f. Closes when f returns nil or throws an exception."
;;   ([f on-fail] (dynamic-source nil nil f on-fail))
;;   ([msecs f on-fail] (dynamic-source nil msecs f on-fail))
;;   ([n msecs f on-fail] (blocking-mult (repeatedly-channel n msecs f on-fail))))

;; (defn polling-source   
;;   "Creates and returns a mult and puts the result of polls onto tapping channels. In case there are
;;   no taps and blocking? is true then polling is paused. If blocking? is false then polling continues
;;   even when there are no taps. Polled values are then dropped.

;;   Poll-fn  must be a function of two arguments: the iteration index and the result value of the
;;   previous poll. The first time it is called values 0 and nil value are provided"
;;   ([poll-fn msecs on-fail] (polling-source poll-fn msecs on-fail nil true))
;;   ([poll-fn msecs on-fail n-or-buf blocking?]
;;      (if blocking? 
;;        (blocking-mult (polling-channel poll-fn msecs on-fail n-or-buf))
;;        (dropping-mult (polling-channel poll-fn msecs on-fail n-or-buf)))))

;; ;;; IV. Sinks (Mixers) and Accumulators
;; ;;; ===================================

;; (defn sink 
;;   "Creates and returns a mix. Accept-fn is called on incoming values from connected channels."
;;   ([accept-fn] (sink accept-fn nil))
;;   ([accept-fn n-or-buf] 
;;      (let [out (as/chan)
;;            m (as/mix out)]
;;        (as/go (loop [next (as/<! out)]
;;                 ;; (println "got" next)
;;                 (if (nil? next)
;;                   (as/close! out)
;;                   (do (accept-fn next)
;;                       (recur (as/<! out))))))
;;        m)))

;; (defn file-sink 
;;   "Creates and returns a mix. Values from connected channels spit to destination. Takes the same
;; options as spit."
;;   ([destination & opts]
;;      (sink (fn [v] (apply spit destination v opts)))))

;; (defn print-sink 
;;   "Creates and returns a mix. Values from connected channels are printed and then discared."
;;   ([] (print-sink nil))
;;   ([n-or-buf] 
;;      (sink (fn [v] (print v)) n-or-buf)))

;; (defn tick-indexer
;;   "A sink accepting ticks and storing them to elastic"
;;   [es-connection]
;;   (sink (fn [tick] 
;;           (try (when (instance? kraken.model.Tick tick)
;;                  (es/index-tick es-connection tick))
;;                (catch Exception e nil))))) ;; TODO: properly work out elastic failure

;; (defn spread-indexer
;;   "A sink accepting spreads and storing them to elastic"
;;   [es-connection]
;;   (sink (fn [spread] 
;;           (try (when (instance? kraken.model.Spread spread)
;;                  (es/index-spread es-connection spread))
;;                (catch Exception e nil)))))

;; (defn trade-indexer
;;   "A sink accepting trades and storing them to elastic"
;;   [es-connection]
;;   (sink (fn [trade]
;;           ;; (println "got" trade)
;;           (try (when (instance? kraken.model.Trade trade)
;;                  (es/index-trade es-connection trade))
;;                (catch Exception e nil)))))

;; (defn accumulator
;;   "Consumes a channel and returns an atom that will be filled with the sequence of values sent
;; through it, newest values first. If size is given, only the last size elements are kept."
;;   ([ch] (accumulator nil ch))
;;   ([size ch]
;;      (let [accumulated (atom nil)]
;;        (as/go-loop [next (as/<! ch)]
;;                    (when-not (nil? next)
;;                      (swap! accumulated conj next)
;;                      (when size
;;                        (swap! accumulated take size))
;;                      (recur (as/<! ch))))
;;        accumulated)))


;; ;; ;;; Monitoring
;; ;; (defn monitor-channel [ch]
;; ;;   (as/admix (print-sink) ch)) ;; problem: need to return the print-sink
;; ;; (defn monitor-source [ch]
;; ;;   (as/admix (print-sink) ch))
