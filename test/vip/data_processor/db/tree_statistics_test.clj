(ns vip.data-processor.db.tree-statistics-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.db.tree-statistics :refer :all]))

(deftest camel->snake-test
  (testing "got the regex close"
    (are [x y] (= x y)
      "snake_from_camel" (camel->snake "SnakeFromCamel"))))
