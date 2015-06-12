(ns vip.data-processor.db.statistics-test
  (:require [clojure.test :refer :all]
           [vip.data-processor.db.statistics :refer :all]))

(deftest error-count-test
  (let [table-name :contests
        ctx {:warnings {table-name {:simple-map {:missing-header "Header missing"
                                                 :too-long "Table seems way too long"}}}
             :errors {table-name {:nested-maps {:bad-names {"Charleeee" "Too many E's"
                                                            "" "Too short"
                                                            "Christopher" "Already taken"}}}}
             :critical {table-name {:sets-of-sets #{#{1 2} #{3 4} #{5 6} #{7 8} #{9 10}}
                                    :maps-to-vectors {:three-problems [1 2 3]
                                                      :four-problems [1 2 3 4]}}}
             :fatal {:this-one-doesnt-have-that-table ["So it will contribute 0 to the count"]}}]
    (is (= 17 (error-count table-name ctx)))))
