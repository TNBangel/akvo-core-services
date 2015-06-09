(ns akvo-unified-log.consumer)

(defprotocol IConsumer
  (-start [consumer])
  (-stop [consumer])
  (-offset [consumer]))

(defn start
  "Start the consumer"
  [consumer]
  (-start consumer))


(defn stop
  "Stop the consumer"
  [consumer]
  (-stop consumer))
