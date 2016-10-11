(ns vip.data-processor.s3-test
  (:require [vip.data-processor.s3 :refer :all]
            [clojure.test :refer [deftest testing is]]))

(deftest zip-filename*-test
  (testing "nicely formatted dates make nice filenames"
    (is (= "vipfeed-51-VA-2015-03-24.zip"
           (zip-filename* "51" "VA" "2015-03-24"))))

  (testing "workably formatted dates make nice filenames"
    (is (= "vipfeed-52-FL-2015-03-27.zip"
           (zip-filename* "52" "FL" "2015/03/27"))))

  (testing "poorly formatted dates use a workable filename"
    (is (= "vipfeed-12345-North-Carolina-.zip"
           (zip-filename* "12345" "North Carolina" nil))))

  (testing "missing the fips and/or state doesn't break the world"
    (is (= "vipfeed-12-YY-2016-11-08.zip"
           (zip-filename* "12" nil "2016-11-08")))
    (is (= "vipfeed-XX-North-Carolina-2016-11-08.zip"
           (zip-filename* "" " North Carolina " "2016-11-08")))))
