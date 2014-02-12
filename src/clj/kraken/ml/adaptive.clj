(ns kraken.ml.adaptive
  (:use [kraken.channels])
  (:require [clojure.core.async :as as]
            [clj-time.core :as tcore]
            [clj-time.coerce :as tcoerce]))

(defrecord adaptor [interval action strength])

(defn in-interval? [value interval]
  (and (< (first interval) value)
       (<= value (second interval))))

(defn fires? [adaptor value] 
  (in-interval? value (:interval adaptor)))

(defn fire [fired adaptor]
  (conj fired adaptor))

(defn compute-fired [adaptors value]
  (reduce (fn [fired adaptor] 
            (if (fires? adaptor value)
              (fire fired adaptor)
              fired))
          #{}
          adaptors))

(defn sum-fired [fired]
  (reduce (fn [sum adaptor] (update-in sum [(:action adaptor)] (fnil #(+ % (:strength adaptor)) 0)))
          {}
          fired))

(defn select-action [action-strengths]
  (let [action-strengths (sort-by val > action-strengths)
        total-strength (reduce + (vals action-strengths))
        r (rand total-strength)]
    (loop [cur-action (key (first action-strengths))
           acc-strength (val (first action-strengths))
           remaining (rest action-strengths)]
      (if (<= r acc-strength)
        cur-action
        (recur (key (first remaining))
               (+ acc-strength (val (first remaining)))
               (rest remaining))))))

(defn update-adaptors [adaptors selected-action fired alpha payoff]
  (let [fired (into #{} fired)
        selected (into #{} (filter #(= selected-action (:action %)) fired))
        total-strength (reduce + (map :strength fired))]
    (map (fn [adaptor]
           (cond (selected adaptor) (update-in adaptor [:strength] #(+ (* alpha %) (* (- 1 alpha) payoff) (* 1e-3 (rand))))
                 (fired adaptor) (update-in adaptor [:strength] #(+ (* alpha %) (* 1e-3 (rand))))
                 :else adaptor))
         adaptors)))

(defn rand-elt [coll]
  (nth coll (rand-int (count coll))))

(defn random-adaptor [min-value max-value actions initial-strength]
  (let [a (+ min-value (rand (- max-value min-value)))
        b (+ min-value (rand (- max-value min-value)))]
    (adaptor. [(min a b) (max a b)] (rand-elt actions) initial-strength)))

(defn random-adaptors [n min-value max-value actions initial-strength]
  (conj (for [i (range (dec n))] (random-adaptor min-value max-value actions initial-strength))
        (adaptor. [min-value max-value] (rand-elt actions) initial-strength)))


(defn in-eur [state price]
  (+ (:eur state) (* (:ltc state) price)))

(defn in-ltc [state price]
  (+ (:ltc state) (/ (:eur state) price)))

(defn perform-action [state value action]
  (merge state (cond (= action :sell) {:eur (in-eur state value)
                                       :ltc 0}
                     (= action :buy) {:eur 0
                                      :ltc (in-ltc state value)}
                     :else {})))

(defn randomized-sinusoidal [& {:keys [period signal-amplitude noise-amplitude]
                                :or {period (* 1000.0 60.0 60.0)
                                     signal-amplitude 1.0
                                     noise-amplitude 0.1}}]
  ;; computes randomized sinusoidal
  (+ (* signal-amplitude
        (Math/sin (/ (double (tcoerce/to-long (tcore/now)))
                     period)))
     (* noise-amplitude (rand noise-amplitude))))

(def state (atom nil))
(def state (atom {:eur 1.0 :ltc 1.0 :adaptors (apply concat (for [i (range 10)]
                                                              (for [action [:buy :sell :wait]]
                                                                (adaptor. [(+ 1.0 (* i 0.1)) (+ 1.0 (* (inc i) 0.1))] action 1.0))))}))
(def state (atom {:eur 1.0 :ltc 1.0 :adaptors [(adaptor. [1.0 1.1] :buy 1.0)
                                               (adaptor. [1.1 1.9] :wait 1.0)
                                               (adaptor. [1.9 2.0] :sell 1.0)]}))
(def state (atom {:eur 1.0 :ltc 1.0 :adaptors [(adaptor. [1.0 1.1] :buy 1.0) (adaptor. [1.0 1.1] :sell 0.1) (adaptor. [1.0 1.1] :wait 0.1)
                                               (adaptor. [1.1 1.9] :wait 1.0) (adaptor. [1.1 1.9] :sell 0.1) (adaptor. [1.1 1.9] :buy 0.1)
                                               (adaptor. [1.9 2.0] :sell 1.0) (adaptor. [1.9 2.0] :buy 0.1)(adaptor. [1.9 2.0] :wait 0.1)]}))
(def state (atom {:eur 1.0, :ltc 0, :adaptors (random-adaptors 10 0 1 [:buy :sell :wait] 1.0)}))
(loop [price (+ 1.5 (randomized-sinusoidal :signal-amplitude 0.5 :noise-amplitude 0.0 :period 10000))
       i 0]
  (Thread/sleep 10)
  (let [actions [:buy :sell :wait]
        fired (compute-fired (:adaptors @state) price)
        d (println fired)
        selected-action (select-action (sum-fired fired))
        next-price (+ 1.5 (randomized-sinusoidal :signal-amplitude 0.5 :noise-amplitude 0.0  :period 100))
        next-states (zipmap actions
                            (map #(perform-action @state price %) actions))
        prices (zipmap (keys next-states) 
                       (map #(in-eur % next-price) (vals next-states)))
        min-price (reduce min (vals prices))
        max-price (reduce max (vals prices))
        d (println (get prices selected-action))
        payoff (/ (- (get prices selected-action) min-price)
                  (- max-price min-price))]
    (println (format "Price: %4.3f -> %4.3f => {:sell %3.2f, :wait %3.2f, :buy %3.2f} ; selected: %s ; %3.2f ; {:eur %3.2f, :ltc %3.2f} => %4.3f" 
                     price next-price (:sell prices) (:wait prices) (:buy prices) selected-action payoff (double (:eur (get next-states selected-action))) (double (:ltc (get next-states selected-action))) (in-eur (get next-states selected-action) next-price)))
    (reset! state (update-in (get next-states selected-action) [:adaptors]
                             #(update-adaptors % selected-action fired 0.8 payoff)))
    (if (< i 1000)
      (recur next-price (inc i))
      @state)))

;;; Seems to work with appropriate initial state and parameters...

;;; things to investigate further:
;;; - other features (direction of change)
;;; - how to combine features
;;; - how to get to categories

;;; It's not yet working with just any initial set of adaptors. The immediate payoff just doesn't
;;; say so much. Neither does the price value. 

;;; There is a winning strategy that is easily described. It says "sell if high", "wait when
;;; intermediate" and "buy when low". The crux is of course to gigure out what is "high" and what is
;;; "low". So suppose that observed prices are remembered and associated with action scores. The
;;; score represent the expected payoff. A strategy that associates a selling action with the lowest
;;; point is bad because any strategy doing just this differently will be at least as good, and
;;; possibly better. At the point before the lowest point, price is dropping, so we must sell. At
;;; the point just after, price is increasing, so we must buy. These two points are the same however
;;; (they occur at the same price). The expected payoff of sell and buy should thus more or less be
;;; the same and smaller than what is expected at the lowest point.

;;; Now considering the lowest point and its predecessor together, any strategy that buys at the
;;; predecessor prevents a buy action at the lowest point, and is thus also bad in this
;;; respect. 

start     (D,0)    (0,E)

buy,buy   (D,0)    (D,0)

buy,sell  (0,E-)    (0,E--)

sell,buy  (D+,0)    (D+,0)

sell,sell (0,E)     (0,E)








t(k-1)
 buy 

       b     
(d(-1),0) ---> (d(k),0) ---> (D,0)
;;;           s          b          b

;;;         t(k-1)      t(k)      t(k+1)
;;;           b          b          b 
;;; (D,0)   (D,0)      (D,0)      (D,0)   
;;; (0,E)   (d,0)      (d,0)      (d,0)   
;;;           b          b          s 
;;; (D,0)   (D,0)      (D,0)      (0,e)   
;;; (0,E)   (d,0)      (d,0)      (D,0)   
;;;           b          s          b 
;;; (D,0)   (D,0)      (D,0)      (D,0)   
;;; (0,E)   (D,0)      (D,0)      (D,0)   
;;;           b          s          s 
;;; (D,0)   (D,0)      (D,0)      (D,0)   
;;; (0,E)   (D,0)      (D,0)      (D,0)   
;;;           s          b          b 
;;; (D,0)   (D,0)      (D,0)      (D,0)   
;;; (0,E)   (D,0)      (D,0)      (D,0)   
;;;           s          b          s 
;;; (D,0)   (D,0)      (D,0)      (D,0)   
;;; (0,E)   (D,0)      (D,0)      (D,0)   
;;;           s          s          b 
;;; (D,0)   (D,0)      (D,0)      (D,0)   
;;; (0,E)   (D,0)      (D,0)      (D,0)   
;;;           s          s          s 
;;; (D,0)   (D,0)      (D,0)      (D,0)   
;;; (0,E)   (D,0)      (D,0)      (D,0)   

;;; b(D,0) = (D,0)
;;; s(D,0) = (0,E)
;;; b(0,E) = (D,0)
;;; s(0,E) = (0,E)



;;; Input is a price. From this, the change of price can be computed, etc. Suppose each next level
;;; tries to compute the next value of the previous level. At the lowest level, there are inputs
;;; generated by the environment. This means that there are two pathways: one "up" from
;;; environmental stimuli to higher and higher derivatives of it and one "down" from actions to
;;; predictions of how things will change ?????


;;; The thing is that alternative strategies must be tried in "similar circumstances". So we start
;;; from a single category ("all and nothing"). We then observe certain points, thereby dividing the
;;; all or nothingness into regions. For every point, a random action is chosen, and the direct
;;; payoff is used to estimate the payoff for that action. After some time, we may find that in
;;; certain regions, some actions yield a higher payoff. 

;;; In particular, near the lowest point, it will surface that buying is a good idea. First, at the
;;; absolute lowest point, only buying can yield payoff, and only if capital is available.  Near a
;;; middle point, there will be equal payyofs for high and low (*eventhough* these events are
;;; separated in time). Moreover, when accumulated, payoffs will be low compared to the payoff
;;; received near the lowest point (they will be canceled out instead of reinforced in a particular
;;; direction -> accumulate (integrate) derivative of capital!! I.e.: when we sell, and price goe
;;; down, we win. If price goes up, we lose. In the first case, we get a positive value, and the
;;; integral increases. In the second case, we get a negative value and the integrsl
;;; decreases.). All in all, expected payoffs for both buy and sell will be measurably low,
;;; suggesting to do nothing at all.

;;; question: when given a random sequence of numbers between -1 and 1, what sampling mechanism
;;; gives zero? 

(def values (for [t (range 1000)]
              (Math/sin (* t 0.01))))
(def alpha 0.999999999)
(defn compute [values]
  (reduce (fn [r n]
            (+ (* r alpha) (* (- 1 alpha) n)))
          0
          values))

(require '[incanter.core :as icore])
(require '[incanter.charts :as ichart])
(def cv (for [i (range 1000)]
          (compute (take (inc i) values))))
(icore/view
 (ichart/xy-plot (range 1000)
                 cv))

;;; This can be realized by associating a threshold below which no action bids are accepted.


;;; 1) -Inf,+inf -> [s1,s2]
;;; 2) observe low, decreasing price p1 
;;;   -Inf       p1               +Inf
;;;
;;;    s1,s2   s1,s2              s1,s2
;;;
;;; 3) s1=s2=... => play sell -> payoff a 
;;;
;;;  -Inf        p1            +Inf
;;;   s1,s2     a,s2           s1,s2

;;; Start with very high initial values, so that all actions are at least triend once.

;;; Probability for an action is weighted average of local probability and neighbouring probabilities.

;;; 


;;; An action brings the complete system in a novel state.

;;; An "action" is an adaptor: it "locks" onto certain triggers and produces an effect. It's only
;;; certain combinations of adaptors that together make a good strategy. Input is a price
;;; level. Specific prices are mapped to other things


p1     p2     p3     p4

pp2    pp3   pp4

ppp3  ppp4    

pppp5

;;; With this trick, we actually get a prediction of states many time frames away! 
;;; Boundary cases:

;;; - a predictor is bad at predicting, but the error it makes is very predictable 

;;; - a prediction must always be based on a current state, and can generaly be improved if the state
;;;   contains differentials etc.

;;; - we want to keep each level as simplt as possible

;;; Simplest prediction: a*(current value.) In this case, the second level's prediction value will
;;; be just as bad as the first one etc. Next simplest prediction: a*(current value) + b*dcv/dt. Of
;;; course, we don't know dcv/dt. We only know p0, p1, ..., pc and, after the first step also ppc+1.

;;; We must compare prediction to actual. In this simplest case, we must compare ppi+1 with
;;; pi+1. This produces a second stream of first level prediction errors. If in turn this stream can
;;; be predicted, the error can perhaps be removed or in any case be taken into account.

;;; What is to be reware=ded of a hierarchy of levels is the overall prediction accuracy. It doesn't
;;; matter if some level is always wrong, as long as it produces very predictable errors.

;;; Note: derivative is obtained with an input channel and a "predictor" that simpky repeats the
;;; input with a time delay of one!!



;;; Now imagine we have an adaptor that says "buy if pp2 > p1" and one that says "buy if pppp5 >
;;; p1". For some i, "buy if p^i > p1" will be optimal in the sense that in the long run, that is
;;; accumulated over time, the payoff is maximised. This suggest that we should measure the success
;;; of any entity by the integral or total payoff.


(defn payoff [ev action])

(rand-elt [:buy :sell :wait])
(random-adaptor 0 1 [:buy :sell :wait] 1.0)
(def adaptors (random-adaptors 5 0 1 [:buy :sell :wait] 1.0))
(def fired (compute-fired adaptors 0.3))
(def selected-action (select-action (sum-fired fired)))
(update-adaptors o action fired 0.9 1.0)
(select-action )
(group-by :action fired)
(count fired)
(fires? (first o) -1)
(fires? (first o) 0.5)
(fires? (first o) 1.5)
(compute-fired )



(def agents )
(defn )

;;; Estimate direct payoffs. If very different -> adjust. If accurate -> propagate to earlier action.

;;; alternatives

;;; cross-over

;;; [p1 p2 p3 ... pn]

;;; associate list of actions with every interval.

;;; Normaal: 

;;; 2 kleuren => nieuwe distinctie als niet discrimineerbaar + random woord
;;; als success => hou 1 woord
;;; ontstaan van linguistic categories is een populatie effect

;;; Hier: 1 prijs, expected payoff

;;; Different strategies employed by different agents. 

;;; 




(defn )

;;; There are three possible actions: sell, buy and do noting.
;;; At any given point in time, one of these three actions is selected.
;;; This is done based on an estimate of the expected return

;;; There are two problems to take care of:

;;; 1) the set of variables that can be taken into account is open ended
;;; 2) the payoff for an action may only be known at a later time

;;; First investigate simplest case: one input only, i.e. current price or its derivative or ....

;;; Approach: adaptively partition price range and map to action probabilities while adjusting
;;; the temporal resolution to get {high-range -> sell, low-range -> buy, middle-range -> keep}.

;;; Start with a random mapping from a random partitioning of prices to actions and a fixed sampling
;;; rate. Then when in the next time step we gain money, we increase the probability for the action
;;; in the partitioning.

;;; Two interesting and probably related issues: how long is a "time step" and what is the optimal
;;; "granularity" of price categories? These should be determined dynamic. In both cases, we need to
;;; make a distinction between the maximum possible resolution (e.g. 50Hz), the stimulus resolution
;;; and the categorial resolution. Unless progress is made, something should change.

;;; Assume we measure prices randomly between l and h. Every time we see a new price, we make an
;;; entry for it, corresponding to a "voronoi" region around the observed point. The entry belongs
;;; to the same "cognitive" category as its enclosing parent, meaning that the same action
;;; distribution is played when it is observed. This process continues until a maximum resolution is
;;; reached beyond which the agent becomes blind to further distinctions. Now we receive payoff. If
;;; it is positive, nothing changes. If it is negative however, the action distribution is
;;; randomized within the new sub-region.

;;; - Instream of prices, ...



;;; - Actions

(defn random-action [actions]
  (nth actions (rand-int (count actions))))

(defn normalize [coll]
  (let [s (reduce + coll)]
    (map #(/ % s) coll)))

(defn random-action-scores [actions]
  (let [v (map (fn [_] (rand)) actions)]
    (zipmap actions (normalize v))))

(defn uniform-action-scores [actions]
  (zipmap actions (repeat (count actions) (/ 1 (count actions)))))

;;; - State 

(defn initial-state [actions]
  [{:value 0
    :action-scores (uniform-action-scores actions)}
   {:value 1e99
    :action (uniform-action-scores actions)}])

(defn get-action-scores [state value]
  (loop [prev (first state)
         remaining (rest state)]
    (let [next (first remaining)]
      (if (<= value (:value next))
        (:action next)
        (recur next (rest remaining))))))

(defn get-action [state value]
  (let [dice (rand 1.0)]
    (loop [action-scores (seq (get-action-scores state value))
           cumm-value 0]
      (cond (empty? action-scores) (throw (Exception. "empty action scores"))
            (>= cumm-value dice)   (:action (first action-scores))
            :else                  (recur (rest action-scores) (+ cumm-value (:value (first action-scores))))))))

(defn randomize-action-scores [scores T]
  (let [r (if (zero? T)
            0.0
            (Math/exp (- (/ 1.0 T))))]
    (println (keys scores))
    (println (vals scores))
    (println (map (partial + r) (vals scores)))
    (println (normalize (map (partial + r) (vals scores))))
    (zipmap (keys scores)
            (normalize (map (partial + r) (vals scores))))))

(defn entrench-action-scores [scores p]
  (zipmap (keys scores)
          (normalize (map #(Math/pow % p) (vals scores)))))

;;; get price -> perform action -> get price -> perform action

;;; If an action has an immediate effect, then we can do an update depending on effect

;;; We might also remember the actions performed. The immediate effect will anyway be small. As time
;;; goes by, an action will move to the tail of the action list, but effects may become large. 

(defn update-state [state price threshold T]
  ;;; should randomize scores in new intervals depending on temperature
  ;;; this correspondents to a default "failure"
  ;;; During entrenchment, existing trends are strengthent
  (first
   (reduce (fn [[new-state found?] next]
             (if found? 
               [(conj new-state next) true]
               (if (<= price (:price next))
                 (let [prev (last new-state)
                       left (/ (+ (:price prev) price) 2)
                       new-state (if (>= (- left (:price prev)) threshold)
                                   (conj new-state {:price left :action (:action prev)})
                                   new-state)
                       right (/ (+ price (:price next)) 2)
                       new-state (if (>= (- (:price next) right) threshold)
                                   (conj new-state {:price right :action (:action next)})
                                   new-state)]
                   [(conj new-state next) true])
                 [(conj new-state next) found?])))
           [[] false]
           state)))

(get-action-scores (initial-state [:buy :sell :wait]) 10)
(reduce #(entrench %1 (inc %2) 1) 
        (initial-state)
        (range 2))



;;; now 
(entrench (entrench (initial-state) 5.0 1)
          10 1)
(def state (entrench (initial-state) 5.0 1))
(def price 10)
(def threshold 1)
;;; Now with each interval we associate action scores

(defn )

                  
                  (if ())
                    (conj new-boundaries prev)))
                  
                    (conj new-boundaries prev left ri)
                  )))])
              (conj ret in category)
              (recur next (rest remaining)))
            (if )))
  (loop [prev (first ontology)
         remaining (rest ontology)]
    (let [next (first ontology)]
      (if (<= (:end in) (:end (:in next)))
        [prev next]
        (recur next (rest remaining)))))))
      

[{:end }]

(defn categorize [ontology observation]
  "returns an updated ontology and a "
  )

;;; - State (amount of money, categories, ...)

;;; - Actions (buy, sell, ...)



;; in the end we want an agent that is part of a go loop, i.e. receiving messages from input channels
;; and sending messages to output channels:


;; request channel serves [request response-channel] messages
;; poison-pill-channel can be given explicitely. 
;; we can also serve a :shutdown request.
;; more generally we can pass arround [request data response-channel] messages
;; If we don't know how to handle a request we simply pass it to output.
;; If we do now we send the response to response channel and to output

;; The environment does stuff on its own, which includes updating state and sending messages. It
;; must also receive messages, e.g. from agents or from a controler channel.

;; Agents also do stuff on their own, as well as send and receive messages to the environment.

            Control       Agent       Env
Control        -        :shutdown  :shutdown
Agent          -            -      :buy,:sell
Env            -          :tick        -

(defn agent [control in out handler]
  )

;;; let's first make a function for computing events



(defprotocol AgentProtocol
  (percieve [state input])
  (respond [state input])
  (adjust [state payoff])
  (act [state]))

(defn start-agent [agent input-channel output-channel reward-channel control-channel]
  (let [agent (agent agent)]
    (as/go-loop [refresh (as/timeout (max (get @agent :min-rate 100)
                                          (get @agent :rate 1000)))]
       (let [[message c] (as/alts! [control-channel
                                    input-channel 
                                    reward-channel
                                    refresh]
                                   :priority true)]
         (condp c =
           control-channel nil
           input-channel (do (send agent percieve message)
                             (as/>! response-channel (respond @agent message))
                             (recur refresh))
           reward-channel (do (send agent adjust message)
                              (recur refresh))
           refresh (let [action (act output-channel)
                       (recur (as/timeout (max (get @agent :min-rate 100)
                                               (get @agent :rate 1000))))))))))
(as/alts!! [(as/timeout 2000) (as/timeout 1000)])
;;; agent without memory is random:
(defn random-agent [actions]
  (reify AgentProtocol
    (percieve [state input] state)
    (respond [state input] (nth actions (rand-int (count actions))))
    (adjust [state input] state)))
;; (* 0.38659475 493)
(* 0.04 493)
(* 187000 0.00000142 500) 130
(* 187000 0.00000236 500) 202
          0.0000019
;;; now make one that keeps scores for actions
;;; then one thay keeps scores for different "categories"

(defn entrench [scores selected alpha mu]
  (reduce (fn [ret action]
            (if (= action selected)
              (update-in ret [action] 
                         #(+ (* alpha %) (* (- 1 alpha) mu))])))
        (update-in scores))

;;; agent that remembers the last payoff and action, and adjusts actions scores accordingly:
(defrecord MaxAgent [last-payoff last-action action-scores alpha]
  AgentProtocol
  (percieve [state input] state)
  (respond [state input] (roulette action-scores))
  (adjust [state payoff]
    (-> state
        (update-in [:action-scores] 
           #(let [delta (- payoff score)]
              (if (pos? delta)
                (entrench % last-action alpha delta)
                (randomize % last-action alpha delta))))
        (assoc :last-payoff payoff)
        (assoc :last-action last-action))))


;;; More thoughts: 

;;; - think of learning to learn to walk around in a maze.  The only way to avoid having to make
;;;   random decisions all the time is to try and remember places together with what is the best
;;;   route from them. We keep a list of the last actions taken. When we receive payoff, the last
;;;   action taken uses it to adjust weights. After some time, not much adjustment wil be required,
;;;   and more adjustments can be performed on earlier actions. This way we onliy need to remember
;;;   decision points about which we are not so certain. This is a kind of modulated
;;;   backpropagation.



    
  
(let [state {:rate 1000
             :min-rate 100}]
  (as/go-loop []
    (let [input (as/alt! (<! input-channel) (<! (as/timout (:rate state))))]
      (swap! state (percieve input))
      (as/>! output-channel (response state input))
      (recur))))

;; but then they also need to recieve rewards


      
      (as/>! output-channel (response ))

;;; clojure agent: a "state" that changes state by sending it a message on how to change it :-)


(defn probe [agent stimulus]
  "Returns a new agent probed with a stimulus."
  (send agent percieve stimulus)
  ()
  (let [perception (percieve agent stimulus)
        category (categorize agent perception stimulus)
        response (reaction agent category )])
  )
