(ns vip.data-processor.validation.v5.street-segment-test
  (:require [vip.data-processor.validation.v5.street-segment :as v5.ss]
            [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.db.postgres :as psql]
            [vip.data-processor.validation.xml :as xml]))

(use-fixtures :once setup-postgres)
(use-fixtures :each with-clean-postgres)

(deftest ^:postgres validate-no-missing-odd-even-both
  (let [ctx {:input (xml-input "v5-street-segment.xml")}
        out-ctx (-> ctx
                    psql/start-run
                    xml/load-xml-ltree
                    v5.ss/validate-no-missing-odd-even-both)]
    (testing "odd-even-both missing is an error"
      (is (get-in out-ctx [:errors :street-segment
                           "VipObject.0.StreetSegment.0.OddEvenBoth" :missing])))
    (testing "odd-even-both present is OK"
      (is (not (get-in out-ctx [:errors :street-segment
                                "VipObject.0.StreetSegment.1.OddEvenBoth"
                                :missing]))))))

(deftest ^:postgres validate-odd-even-both-value
  (let [ctx {:input (xml-input "v5-street-segment.xml")}
        out-ctx (-> ctx
                    psql/start-run
                    xml/load-xml-ltree
                    v5.ss/validate-odd-even-both-value)]
    (testing "odd/even/both are good values"
      (doseq [path ["VipObject.0.StreetSegment.1.OddEvenBoth.0"
                    "VipObject.0.StreetSegment.2.OddEvenBoth.0"
                    "VipObject.0.StreetSegment.3.OddEvenBoth.0"]]
        (assert-no-problems out-ctx
                            [:street-segment
                             path
                             :format])))
    (testing "anything else is not"
      (is (get-in out-ctx [:errors
                           :street-segment
                           "VipObject.0.StreetSegment.4.OddEvenBoth.0"
                           :format])))))

(deftest ^:postgres validate-no-missing-city
  (let [ctx {:input (xml-input "v5-street-segment.xml")}
        out-ctx (-> ctx
                    psql/start-run
                    xml/load-xml-ltree
                    v5.ss/validate-no-missing-city)]
    (testing "city missing is an error"
      (is (get-in out-ctx [:errors :street-segment
                           "VipObject.0.StreetSegment.0.City" :missing]))
      (is (get-in out-ctx [:errors :street-segment
                           "VipObject.0.StreetSegment.4.City" :missing])))
    (testing "city present is OK"
      (doseq [path ["VipObject.0.StreetSegment.1.City"
                    "VipObject.0.StreetSegment.2.City"
                    "VipObject.0.StreetSegment.3.City"]]
        (assert-no-problems out-ctx
                            [:street-segment
                             path
                             :missing])))))

(deftest ^:postgres validate-no-missing-state
  (let [ctx {:input (xml-input "v5-street-segment.xml")}
        out-ctx (-> ctx
                    psql/start-run
                    xml/load-xml-ltree
                    v5.ss/validate-no-missing-state)]
    (testing "state missing is an error"
      (is (get-in out-ctx [:errors :street-segment
                           "VipObject.0.StreetSegment.0.State" :missing]))
      (is (get-in out-ctx [:errors :street-segment
                           "VipObject.0.StreetSegment.4.State" :missing])))
    (testing "state present is OK"
      (doseq [path ["VipObject.0.StreetSegment.1.State"
                    "VipObject.0.StreetSegment.2.State"
                    "VipObject.0.StreetSegment.3.State"]]
        (assert-no-problems out-ctx
                            [:street-segment
                             path
                             :missing])))))

(deftest ^:postgres validate-no-missing-zip
  (let [ctx {:input (xml-input "v5-street-segment.xml")}
        out-ctx (-> ctx
                    psql/start-run
                    xml/load-xml-ltree
                    v5.ss/validate-no-missing-zip)]
    (testing "zip missing is an error"
      (is (get-in out-ctx [:errors :street-segment
                           "VipObject.0.StreetSegment.0.Zip" :missing]))
      (is (get-in out-ctx [:errors :street-segment
                           "VipObject.0.StreetSegment.4.Zip" :missing])))
    (testing "zip present is OK"
      (doseq [path ["VipObject.0.StreetSegment.1.Zip"
                    "VipObject.0.StreetSegment.2.Zip"
                    "VipObject.0.StreetSegment.3.Zip"]]
        (assert-no-problems out-ctx
                            [:street-segment
                             path
                             :missing])))))

(deftest ^:postgres validate-no-missing-street-name
  (let [ctx {:input (xml-input "v5-street-segment.xml")}
        out-ctx (-> ctx
                    psql/start-run
                    xml/load-xml-ltree
                    v5.ss/validate-no-missing-street-name)]
    (testing "street-name missing is an error"
      (is (get-in out-ctx [:errors :street-segment
                           "VipObject.0.StreetSegment.0.StreetName" :missing]))
      (is (get-in out-ctx [:errors :street-segment
                           "VipObject.0.StreetSegment.4.StreetName" :missing])))
    (testing "street-name present is OK"
      (doseq [path ["VipObject.0.StreetSegment.1.StreetName"
                    "VipObject.0.StreetSegment.2.StreetName"
                    "VipObject.0.StreetSegment.3.StreetName"]]
        (assert-no-problems out-ctx
                            [:street-segment
                             path
                             :missing])))))
