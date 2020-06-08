(ns vip.data-processor.output.xml-helpers-test
  (:require [vip.data-processor.output.xml-helpers :refer :all]
            [clojure.string :as str]
            [clojure.test :refer :all]))

(deftest format-fips-test
  (testing "empty"
    (is (= "XX" (format-fips "")))
    (is (= "XX" (format-fips nil))))
  (testing "1 digit fips"
    (is (= "01" (format-fips "1"))))
  (testing "2 digit fips"
    (is (= "08" (format-fips "08")))
    (is (= "10" (format-fips "10"))))
  (testing "3 digit fips (shouldn't be many/any of these)"
    (is (= "00008" (format-fips "008")))
    (is (= "00101" (format-fips "101"))))
  (testing "4 digit fips (could be a county fips from a low number state)"
    (is (= "08001" (format-fips "8001"))))
  (testing "5 digit fips (county fips)"
    (is (= "08001" (format-fips "08001")))
    (is (= "10101" (format-fips "10101")))))

(deftest format-state-test
  (testing "empty"
    (is (= "YY" (format-state "")))
    (is (= "YY" (format-state nil))))
  (testing "Single Word State"
    (is (= "Colorado" (format-state "Colorado"))))
  (testing "Two Word State"
    (is (= "New-York" (format-state "New York")))))

(deftest filename*-test
  (testing "nicely formatted dates make nice filenames"
    (is (= "vipfeed-51-VA-2015-03-24"
           (filename* "51" "VA" "2015-03-24"))))

  (testing "workably formatted dates make nice filenames"
    (is (= "vipfeed-52-FL-2015-03-27"
           (filename* "52" "FL" "2015/03/27"))))

  (testing "poorly formatted dates use a workable filename"
    (is (= "vipfeed-12345-North-Carolina-"
           (filename* "12345" "North Carolina" nil))))

  (testing "missing the fips and/or state doesn't break the world"
    (is (= "vipfeed-12-YY-2016-11-08"
           (filename* "12" nil "2016-11-08")))
    (is (= "vipfeed-XX-North-Carolina-2016-11-08"
           (filename* "" " North Carolina " "2016-11-08")))))

(deftest create-xml-file-test
  (testing "adds an :xml-output-file key to a context"
    (let [ctx {:output-file-basename "vipfeed-51-VA-2015-03-24"}
          out-ctx (create-xml-file ctx)]
      (is (:xml-output-file out-ctx))
      (is (str/starts-with?
           (-> (:xml-output-file out-ctx)
               .getFileName
               .toString)
           "vipfeed-51-VA-2015-03-24"))
      (is (= "vipfeed-51-VA-2015-03-24.xml"
             (-> (:xml-output-file out-ctx)
                 .getFileName
                 .toString))))))
