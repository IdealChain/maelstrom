(ns maelstrom.workload.fifo-kv
  "A workload for a fifo-kv key-value store."
  (:refer-clojure :exclude [read test])
  (:require [maelstrom [client :as c]
                       [net :as net]]
            [jepsen [client :as client]
                    [checker :as checker]
                    [independent :as independent]
                    [generator :as gen]]
            [jepsen.checker.timeline :as timeline]
            [knossos.model :as model]
            [schema.core :as s]))

(c/defrpc read
  "Reads the current value of a single key. Clients send a `read` request with
  the key they'd like to observe, and expect a `read_ok` response with the current
  `value` of that key."
  {:type  (s/eq "read")
   :key   s/Any}
  {:type  (s/eq "read_ok")
   :value s/Any})

(c/defrpc write!
  "Blindly overwrites the value of a key. Creates keys if they do not presently
  exist. Servers should respond with a `write_ok` response once the write is
  complete."
  {:type (s/eq "write")
   :key  s/Any
   :value s/Any}
  {:type (s/eq "write_ok")})

(defn client
  "Construct a fifo-kv key-value client for the given network"
  ([net nodes]
   (client net nil nil nodes))
  ([net conn node nodes]
   (reify client/Client
     (open! [this test node]
       (client net (c/open! net) node nodes))

     (setup! [this test])

     (invoke! [_ test op]
       (c/with-errors op #{:read}
         (let [[k v]   (:value op)
               nc      (count nodes)
               timeout (max (* 10 (:mean (:latency test))) 1000)]
           (case (:f op)

             ;; sticky client: process must not switch between nodes
             :read (let [res (read conn node {:key k} timeout)
                         v (:value res)]
                     (assoc op
                            :type  :ok
                            :value (independent/tuple k v)))

             ;; single writer: only allow writes to client/node-owned keys
             :write (let [allowed-node (str "n" (mod k nc))]
                      (cond
                        (not= node allowed-node) (assoc op :type :fail)
                        :else (let [res (write! conn node {:key k, :value v} timeout)]
                                (assoc op :type :ok))))))))

     (teardown! [_ test])

     (close! [_ test]
       (c/close! conn))

     client/Reusable
     (reusable? [this test]
       true))))

;; generated operations: use a unique value for every write
(def write-counter (atom 1))
(defn r   [_ _] {:type :invoke, :f :read})
(defn w   [_ _] {:type :invoke, :f :write, :value (swap! write-counter inc)})

(defn test
  [opts]
  {:checker (independent/checker
             (checker/compose
              {:timeline (timeline/html)}))
   :generator (let [nc (count (:nodes opts))]
                (independent/concurrent-generator
                 (* nc 4) ;; 4 processes per node
                 (cycle (range 0 (* nc 4))) ;; ~4 keys per node
                 (fn [k]
                   (->> (gen/mix [r r w])
                        (gen/limit (:per-key-limit opts 20))))))})


(defn workload
  "Constructs a workload for a fifo-kv KV data store, given option from the CLI
   test constructor:

      {:net     A Maelstrom network}"
  [opts]
  (-> (test {:nodes (:nodes opts)})
      (assoc :client (client (:net opts) (:nodes opts)))))
