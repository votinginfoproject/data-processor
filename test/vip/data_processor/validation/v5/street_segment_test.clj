(ns vip.data-processor.validation.v5.street-segment-test
  (:require [vip.data-processor.validation.v5.street-segment :as v5.ss]
            [vip.data-processor.validation.xml.v5 :as xml.v5]
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

(deftest ^:postgres validate-no-street-segment-overlaps-test
  (let [ctx {:input (xml-input "v5-street-segment-overlaps.xml")}
        out-ctx (-> ctx
                    psql/start-run
                    xml/load-xml-ltree
                    xml.v5/load-xml-street-segments
                    v5.ss/validate-no-street-segment-overlaps)]
    (are [_ path overlaps] (is (= (get-in out-ctx
                                          [:errors
                                           :street-segment
                                           path
                                           :overlaps])
                                  overlaps))
      "simple, complete overlap"    "VipObject.0.StreetSegment.0" '("ss002")
      "partial overlap"             "VipObject.0.StreetSegment.2" '("ss004")
      "single address overlap"      "VipObject.0.StreetSegment.4" '("ss006")
      "even/odd don't overlap"      "VipObject.0.StreetSegment.6" nil
      "even/odd don't overlap"      "VipObject.0.StreetSegment.7" nil
      "even/both overlap"           "VipObject.0.StreetSegment.8" '("ss010")
      "odd/both overlap"            "VipObject.0.StreetSegment.10" '("ss012")
      "shared precinct"             "VipObject.0.StreetSegment.12" nil
      "shared precinct"             "VipObject.0.StreetSegment.13" nil
      "matching more fields"        "VipObject.0.StreetSegment.14" '("ss016")
      "different address direction" "VipObject.0.StreetSegment.16" nil
      "different address direction" "VipObject.0.StreetSegment.17" nil
      "different street direction"  "VipObject.0.StreetSegment.18" nil
      "different street direction"  "VipObject.0.StreetSegment.19" nil
      "different street suffix"     "VipObject.0.StreetSegment.20" nil
      "different street suffix"     "VipObject.0.StreetSegment.21" nil)))
