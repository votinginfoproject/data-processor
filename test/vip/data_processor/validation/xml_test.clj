(ns vip.data-processor.validation.xml-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.validation.xml :refer :all]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.db.sqlite :as sqlite]
            [clojure.java.io :as io]))
