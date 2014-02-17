(defproject kraken "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0-alpha1"]
                 [org.clojure/tools.reader "0.8.0"]
                 [pandect "0.3.0"]
                 [clj-http "0.7.7"]
                 [clj-time "0.6.0"]
                 [org.clojure/data.json "0.2.3"]
                 [org.clojure/core.async "0.1.242.0-44b1e3-alpha"]
                 [ring/ring-core "1.1.8"]
                 [ring/ring-json "0.2.0"]
                 [compojure "1.1.6"]
                 [org.clojure/clojurescript "0.0-2069"]
                 [clojurewerkz/elastisch "1.1.1"]
                 [http-kit "2.1.13"]
                 [com.taoensso/timbre "3.0.1"]]
  :ring {:handler kraken.core/handler}
  :source-paths ["src/clj/kraken" "src/clj" "src/cljs"]
  :plugins [[lein-cljsbuild "1.0.0"]
            [lein-ring "0.8.8"]]
  :profiles {:dev {:dependencies [[ring-serve "0.1.2"]
                                  [ring/ring-devel "1.1.0"]]}}  
  :cljsbuild {:builds
              [{ ;; CLJS source code path
                :source-paths ["src/cljs"]

                ;; Google Closure (CLS) options configuration
                :compiler { ;; CLS generated JS script filename
                           :output-to "resources/public/js/core.js"
                           ;; minimal JS optimization directive
                           :optimizations :whitespace
                           ;; generated JS code prettyfication
                           :pretty-print true}}]})
