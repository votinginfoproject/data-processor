(ns vip.data-processor.validation.data-spec.translate-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.validation.data-spec.translate :refer :all]))

(deftest clean-text-test
  (testing "replaces formfeed with newline"
    (let [bad-text "Hello\fWorld!"
          good-text "Hello\nWorld!"]
      (is (= good-text (clean-text bad-text))))))
