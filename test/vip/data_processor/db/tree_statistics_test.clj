(ns vip.data-processor.db.tree-statistics-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.db.tree-statistics :refer :all]))

(deftest camel->snake-test
  (testing "got the regex close"
    (are [x y] (= (camel->snake x) y)
      "SnakeFromCamel" "snake_from_camel"
       "Oneword" "oneword"
       "ABBRHandledLikeThis" "abbr_handled_like_this"
       "lowercase" "lowercase")))
