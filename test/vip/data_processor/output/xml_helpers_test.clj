(ns vip.data-processor.output.xml-helpers-test
  (:require [vip.data-processor.output.xml-helpers :refer :all]
            [clojure.test :refer :all]))

(deftest create-xml-file-test
  (testing "adds an :xml-output-file key to a context"
    (let [ctx {:filename "create-xml-test"}
          out-ctx (create-xml-file ctx)]
      (is (:xml-output-file out-ctx)))))
