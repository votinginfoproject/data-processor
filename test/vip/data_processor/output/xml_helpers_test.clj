(ns vip.data-processor.output.xml-helpers-test
  (:require [vip.data-processor.output.xml-helpers :refer :all]
            [clojure.string :as str]
            [clojure.test :refer :all]))

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
