(ns vip.data-processor.pipeline-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.pipeline :refer :all]))

(defn incrementor [ctx]
  (update ctx :input inc))
(defn thrower [ctx]
  (throw (Throwable. "Boom!")))

(deftest try-processing-fn-test
  (let [ctx {:input 0}]
    (testing "Without an exception"
      (let [result (try-processing-fn incrementor ctx)]
        (is (= 1 (:input result)))
        (is (not (contains? result :stop)))
        (is (not (contains? result :exception)))))
    (testing "With an exception"
      (let [result (try-processing-fn thrower ctx)]
        (is (:stop result))
        (is (:exception result))))))

(deftest run-pipeline-test
  (testing "Without an exception"
    (let [counter (atom 0)
          ctx {:input 0
               :check-stop-flag-fn #(do (swap! counter inc) identity %)
               :pipeline [incrementor
                            incrementor
                            incrementor]}
          result (run-pipeline ctx)]
      (is (= 3 (:input result)))
      (is (= 3 @counter))
      (is (empty? (:pipeline result)))
      (is (not (contains? result :stop)))
      (is (not (contains? result :exception)))))
  (testing "With an exception"
    (let [ctx {:input 0
               :check-stop-flag-fn #(identity %)
               :pipeline [incrementor
                            thrower
                            incrementor]}
          result (run-pipeline ctx)]
      (is (:stop result))
      (is (:exception result))
      (is (= 1 (:input result)))
      (is (= thrower (:thrown-by result)))
      (is (= [incrementor] (:pipeline result)))))
  (testing "With a stop request"
    (let [ctx {:input 0
               :check-stop-flag-fn #(assoc (identity %) :stop "Stop requested by Fakey McName")
               :pipeline [incrementor
                            incrementor
                            incrementor]}
          result (run-pipeline ctx)]
      (is (:stop result))
      (is (= 1 (:input result)))
      (is (not (empty? (:pipeline result))))
      (is (not (contains? result :exception))))))

(deftest process-test
  (testing "Builds the context and executes the pipeline"
    (let [intial-input {:input 0 :check-stop-flag-fn #(identity %)}
          pipeline [incrementor incrementor incrementor]
          result (process pipeline intial-input)]
      (is (= 3 (:input result)))
      (is (empty? (:pipeline result))))))
