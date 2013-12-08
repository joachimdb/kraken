(ns kraken.channels
  (:require [clojure.core.async :as as]
            [clj-time.core :as tcore]
            [clj-time.coerce :as tcoerce]
            [kraken.api.public :as pub]
            [kraken.elastic :as es]))

;;; I. General channel creation functions
;;; =====================================

(defn repeat-channel 
  "Creates and returns a channel that is filled with x."
  ([x] (repeat-channel nil x))
  ([n x]
     (let [out (as/chan 1)]
       (as/go-loop [i 0]
                   (if (or (nil? n) (> n i))
                     (do (as/put! out x)
                         (recur (inc i)))
                     (as/close! out)))
       out)))

(defn repeatedly-channel 
  "Creates and returns a channel that is filled with repeated calls to f. Closes when f returns nil or throws an exception."
  ([f] (repeatedly-channel nil nil f))
  ([msecs f] (repeatedly-channel nil msecs f))
  ([n msecs f]
     (let [out (as/chan 1)]
       (as/go-loop [i 0]
            (if (or (nil? n) (> n i))
              (let [v (try (f) (catch Exception e nil))]
                (if (nil? v)
                  (as/close! out)
                  (do (as/put! out v)
                      (when msecs 
                        (as/<! (as/timeout msecs)))
                      (recur (inc i)))))
              (as/close! out)))
       out)))

(defn pulse 
  "Creates and returns a channel that outputs true every msecs milliseconds."
  ([msecs] (pulse nil msecs))
  ([n msecs]
     (repeatedly-channel n msecs (repeatedly true) true)
     (let [out (as/chan)]
       (as/go-loop [i 0]
                   (if (or (nil? n) (> n i))
                     (do (as/put! out true)
                         (as/<! (as/timeout msecs))
                         (recur (inc i)))
                     (as/close! out)))
       out)))

(defn polling-channel
  "Creates and returns a channel and puts the result of polls onto it. The channel is closed when a
poll returns nil or throws an exception."
  ([poll-fn msecs] (polling-channel poll-fn msecs nil))
  ([poll-fn msecs n-or-buf]
     (let [i (atom 0)
           prev (atom nil)]
       (repeatedly-channel msecs 
                           (fn []
                             (reset! prev (poll-fn @i @prev))
                             (swap! i inc)
                             @prev)))))


(def +continue+ (atom true))

(defn tick-channel 
  ([pair poll-interval] 
     (assert @+continue+)
     (tick-channel pair poll-interval nil (fn [] @+continue+)))
  ([pair poll-interval n-or-buf continue?]
     (as/mapcat< identity ;; pub/ticker returns a list of ticks
                 (polling-channel (fn [_ _] (when (continue?) (pub/ticker pair))) poll-interval n-or-buf))))

(defn spread-channel 
  ([pair poll-interval] 
     (assert @+continue+)
     (spread-channel pair poll-interval nil (fn [] @+continue+) nil 0))
  ([pair poll-interval on-fail] 
     (assert @+continue+)
     (spread-channel pair poll-interval on-fail (fn [] @+continue+) nil 0))
  ([pair poll-interval on-fail continue? last last-processed]
     (let [ch (as/chan)
           last (atom last)
           last-processed (atom last-processed)
           get-spread (fn []
                        (try (pub/spread pair :since @last)
                             (catch Exception e on-fail)))]
       (as/go-loop []
           (let [next (get-spread)]
             (if (and (continue?) (not (nil? next)))
               (do (when (> (count next) @last-processed)
                       (as/onto-chan ch (drop @last-processed next) false))
                   (reset! last (:last (meta next)))
                   (reset! last-processed (count (filter #(= (* 1000 @last) (tcoerce/to-long (:time %))) next)))
                   (as/<! (as/timeout poll-interval))
                   (recur))
               (as/close! ch))))
       ch)))

(defn trade-channel 
  ([pair poll-interval] 
     (assert @+continue+)
     (trade-channel pair poll-interval (fn [] @+continue+)))
  ([pair poll-interval continue?]
     (as/mapcat< identity ;; pub/trades returns a list of trades
                 (polling-channel (fn [_ prev] (when (continue?) (pub/trades pair (:last (meta prev))))) poll-interval))))


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

(defn differentiate 
  "Creates and returns a channel that outputs the derivative of input channel. Closes when input
channel is closed or when it is not differentiable and close-on-infinite? is true."
  ([channel value-key time-key] (differentiate channel value-key time-key true))
  ([channel value-key time-key close-on-infinite?]
     (let [ch (as/chan)]
       (as/go (loop [prev (as/<! channel)]
                (if (nil? prev)
                  (as/close! ch)
                  (if-let [next (as/<! channel)]
                    (let [dx (- (value-key next) (value-key @prev))
                          dt (- (time-key next) (time-key prev))]
                      (if (zero? dt) 
                        (if close-on-infinite?
                          (as/close! ch)
                          (do (cond (zero? dx) (as/put! ch 0.0)
                                    (pos? dx) (as/put! ch Double/POSITIVE_INFINITY)
                                    :else (as/put! ch Double/NEGATIVE_INFINITY))
                              (recur next)))
                        (do (as/put! ch (double (/ dx dt)))
                            (recur next))))
                    (as/close! ch)))))
       ch)))

(defn integrate [channel value-key time-key]
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


;;; III. Sources (mults)
;;; ====================

(defn dropping-mult
  "Creates and returns a mult(iple) of the supplied channel. Channels
  containing copies of the channel can be created with 'tap', and
  detached with 'untap'.

  Each item is distributed to all taps in parallel and synchronously,
  i.e. each tap must accept before the next item is distributed. Use
  buffering/windowing to prevent slow taps from holding up the mult.

  Items received when there are no taps get dropped.

  If a tap put throws an exception, it will be removed from the mult."
  [ch] 
  (let [cs (atom {}) ;; ch->close?
        m (reify
            as/Mux
            (as/muxch* [_] ch)

            as/Mult
            (as/tap* [_ ch close?] (swap! cs assoc ch close?) nil)
            (as/untap* [_ ch] (swap! cs dissoc ch) nil)
            (as/untap-all* [_] (reset! cs {}) nil))
        dchan (as/chan 1)
        dctr (atom nil)
        done #(when (zero? (swap! dctr dec))
                (as/put! dchan true))]
    (as/go-loop []
                (let [val (as/<! ch)]
                  (if (nil? val)
                    (doseq [[c close?] @cs]
                      (when close? (as/close! c)))
                    (let [chs (keys @cs)]
                      (reset! dctr (count chs))
                      (doseq [c chs]
                        (try
                          (as/put! c val done)
                          (catch Exception e
                            (swap! dctr dec)
                            (as/untap* m c))))
                      ;;wait for all unless no subscriptions
                      (when-not (empty? @cs) (as/<! dchan))
                      (recur)))))
    m))


(defn blocking-mult
  "Creates and returns a mult(iple) of the supplied channel. Channels
  containing copies of the channel can be created with 'tap', and
  detached with 'untap'.

  Each item is distributed to all taps in parallel and synchronously,
  i.e. each tap must accept before the next item is distributed. Use
  buffering/windowing to prevent slow taps from holding up the mult.

  Stops polling when no taps are registered

  If a tap put throws an exception, it will be removed from the mult."
  [ch] 
  (let [cs (atom {}) ;; ch->close?
        cchan (as/chan)
        m (reify
            as/Mux
            (as/muxch* [_] ch)

            as/Mult
            (as/tap* [_ ch close?] (swap! cs assoc ch close?) (as/put! cchan true) nil)
            (as/untap* [_ ch] (swap! cs dissoc ch) nil)
            (as/untap-all* [_] (reset! cs {}) nil))
        dchan (as/chan 1)
        dctr (atom nil)
        done #(when (zero? (swap! dctr dec))
                (as/put! dchan true))]
    (as/go-loop []
                (let [val (as/<! ch)]
                  (if (nil? val)
                    (doseq [[c close?] @cs]
                      (when close? (as/close! c)))
                    (let [chs (keys @cs)]
                      (reset! dctr (count chs))
                      (doseq [c chs]
                        (try
                          (as/put! c val done)
                          (catch Exception e
                            (swap! dctr dec)
                            (as/untap* m c))))                      
                      (when (empty? @cs)
                        (as/<! cchan)) ;; wait until tapped 
                      ;;wait for all 
                      (as/<! dchan)
                      (recur)))))
    m))

(defn constant-source 
  "Creates and returns a source that constantly outputs x."
  ([x] (constant-source nil x))
  ([n x]
     (blocking-mult (repeat-channel n x))))

(defn dynamic-source 
  "Creates and returns a source that is filled with repeated calls to f. Closes when f returns nil or throws an exception."
  ([f] (dynamic-source nil nil f))
  ([msecs f] (dynamic-source nil msecs f))
  ([n msecs f] (blocking-mult (repeatedly-channel n msecs f))))

(defn polling-source   
  "Creates and returns a mult and puts the result of polls onto tapping channels. In case there are
  no taps and blocking? is true then polling is paused. If blocking? is false then polling continues
  even when there are no taps. Polled values are then dropped.

  Poll-fn  must be a function of two arguments: the iteration index and the result value of the
  previous poll. The first time it is called values 0 and nil value are provided"
  ([poll-fn msecs] (polling-source poll-fn msecs nil true))
  ([poll-fn msecs n-or-buf blocking?]
     (if blocking? 
       (blocking-mult (polling-channel poll-fn msecs n-or-buf))
       (dropping-mult (polling-channel poll-fn msecs n-or-buf)))))

(defn tick-source 
  ([pair msecs] 
     (assert @+continue+)
     (tick-source pair msecs nil (fn [] @+continue+)))
  ([pair msecs on-fail] 
     (assert @+continue+)
     (tick-source pair msecs on-fail (fn [] @+continue+)))
  ([pair msecs on-fail continue?]
     (blocking-mult (as/mapcat< identity ;; pub/ticker returns a sequence of ticks 
                                (polling-channel (fn [_ _] (when (continue?) (try (pub/ticker pair) (catch Exception e on-fail))))
                                                 msecs)))))

(defn spread-source
  ([pair msecs] 
     (assert @+continue+)
     (spread-source pair msecs nil (fn [] @+continue+) nil 0))
  ([pair msecs on-fail] 
     (assert @+continue+)
     (spread-source pair msecs on-fail (fn [] @+continue+) nil 0))
  ([pair msecs on-fail continue?] 
     (spread-source pair msecs on-fail continue? nil 0))
  ([pair msecs on-fail continue? last last-processed]
     (blocking-mult (spread-channel pair msecs on-fail continue? last last-processed))))

(defn trade-source 
  ([pair msecs] 
     (assert @+continue+)
     (trade-source pair msecs nil (fn [] @+continue+)))
  ([pair msecs on-fail] 
     (assert @+continue+)
     (trade-source pair msecs on-fail (fn [] @+continue+)))
  ([pair msecs on-fail continue?]
     (blocking-mult (as/mapcat< identity ;; pub/tradeer returns a sequence of trades 
                                (polling-channel (fn [_ prev] (when (continue?) (try (pub/trades pair :since (:last (meta prev))) (catch Exception e on-fail)))) msecs)))))

;;; IV. Sinks (Mixers) and Accumulators
;;; ===================================

(defn sink 
  "Creates and returns a mix. Accept-fn is called on incoming values from connected channels."
  ([accept-fn] (sink accept-fn nil))
  ([accept-fn n-or-buf] 
     (let [out (as/chan)
           m (as/mix out)]
       (as/go (loop [next (as/<! out)]
                ;; (println "got" next)
                (if (nil? next)
                  (as/close! out)
                  (do (accept-fn next)
                      (recur (as/<! out))))))
       m)))

(defn file-sink 
  "Creates and returns a mix. Values from connected channels spit to destination. Takes the same
options as spit."
  ([destination & opts]
     (sink (fn [v] (apply spit destination v opts)))))

(defn print-sink 
  "Creates and returns a mix. Values from connected channels are printed and then discared."
  ([] (print-sink nil))
  ([n-or-buf] 
     (sink (fn [v] (print v)) n-or-buf)))

(defn tick-indexer
  "A sink accepting ticks and storing them to elastic"
  [es-connection]
  (sink (fn [tick] (when tick (es/index-tick es-connection tick)))))

(defn spread-indexer
  "A sink accepting spreads and storing them to elastic"
  [es-connection]
  (sink (fn [spread] (when spread (es/index-spread es-connection spread)))))

(defn trade-indexer
  "A sink accepting trades and storing them to elastic"
  [es-connection]
  (sink (fn [trade] (when trade (es/index-trade es-connection trade)))))

(defn accumulator
  "Consumes a channel and returns an atom that will be filled with the sequence of values sent
through it, newest values first. If size is given, only the last size elements are kept."
  ([ch] (accumulator nil ch))
  ([size ch]
     (let [accumulated (atom nil)]
       (as/go-loop [next (as/<! ch)]
                   (when-not (nil? next)
                     (swap! accumulated conj next)
                     (when size
                       (swap! accumulated take size))
                     (recur (as/<! ch))))
       accumulated)))



