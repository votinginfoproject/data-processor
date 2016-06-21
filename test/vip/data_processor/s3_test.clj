(ns vip.data-processor.s3-test
  (:require [vip.data-processor.s3 :refer :all]
            [clojure.test :refer [deftest testing is]]))

(deftest zip-filename*-test
  (testing "nicely formatted dates make nice filenames"
    (is (= "vipfeed-51-2015-03-24.zip"
           (zip-filename* "51" "2015-03-24"))))

  (testing "workably formatted dates make nice filenames"
    (is (= "vipfeed-52-2015-03-27.zip"
           (zip-filename* "52" "2015/03/27"))))

  (testing "poorly formatted dates use a workable filename"
    (is (= "vipfeed-12345-.zip"
           (zip-filename* "12345" nil)))))
