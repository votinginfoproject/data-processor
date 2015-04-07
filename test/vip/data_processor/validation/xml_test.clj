(ns vip.data-processor.validation.xml-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.validation.xml :refer :all]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.db.sqlite :as sqlite]
            [clojure.java.io :as io]))

(deftest parse-xml-test
  (let [ctx {:input [(io/as-file (io/resource "xml/full-good-run.xml"))]}
        out-ctx (parse-xml ctx)]
    (is (instance? clojure.data.xml.Element (:xml out-ctx)))))
