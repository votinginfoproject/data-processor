(ns vip.data-processor.validation.v5.street-segment-test
  (:require [vip.data-processor.validation.v5.street-segment :as v5.ss]
            [vip.data-processor.validation.xml.v5 :as xml.v5]
            [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.db.postgres :as psql]
            [vip.data-processor.validation.xml :as xml]
            [clojure.core.async :as a]))

(use-fixtures :once setup-postgres)

(deftest ^:postgres validate-no-missing-odd-even-both
  (let [errors-chan (a/chan 100)
        ctx {:xml-source-file-path (xml-input "v5-street-segment.xml")
             :errors-chan errors-chan}
        out-ctx (-> ctx
                    psql/start-run
                    xml/load-xml-ltree
                    v5.ss/validate-no-missing-odd-even-both)
        errors (all-errors errors-chan)]
    (testing "odd-even-both missing is an error"
      (is (contains-error? errors
                           {:severity :errors
                            :scope :street-segment
                            :identifier "VipObject.0.StreetSegment.0.OddEvenBoth"
                            :error-type :missing})))
    (testing "odd-even-both present is OK"
      (assert-no-problems errors
                          {:severity :errors
                           :scope :street-segment
                           :identifier "VipObject.0.StreetSegment.1.OddEvenBoth"
                           :error-type :missing}))))

(deftest ^:postgres validate-odd-even-both-value
  (let [errors-chan (a/chan 100)
        ctx {:xml-source-file-path (xml-input "v5-street-segment.xml")
             :errors-chan errors-chan}
        out-ctx (-> ctx
                    psql/start-run
                    xml/load-xml-ltree
                    v5.ss/validate-odd-even-both-value)
        errors (all-errors errors-chan)]
    (testing "odd/even/both are good values"
      (doseq [path ["VipObject.0.StreetSegment.1.OddEvenBoth.0"
                    "VipObject.0.StreetSegment.2.OddEvenBoth.0"
                    "VipObject.0.StreetSegment.3.OddEvenBoth.0"]]
        (assert-no-problems errors
                            {:scope :street-segment
                             :identifier path
                             :error-type :format})))
    (testing "anything else is not"
      (is (contains-error? errors
                           {:severity :errors
                            :scope :street-segment
                            :identifier "VipObject.0.StreetSegment.4.OddEvenBoth.0"
                            :error-type :format})))))

(deftest ^:postgres validate-no-missing-city
  (let [errors-chan (a/chan 100)
        ctx {:xml-source-file-path (xml-input "v5-street-segment.xml")
             :errors-chan errors-chan}
        out-ctx (-> ctx
                    psql/start-run
                    xml/load-xml-ltree
                    v5.ss/validate-no-missing-city)
        errors (all-errors errors-chan)]
    (testing "city missing is an error"
      (is (contains-error? errors
                           {:severity :errors
                            :scope :street-segment
                            :identifier "VipObject.0.StreetSegment.0.City"
                            :error-type :missing}))
      (is (contains-error? errors
                           {:severity :errors
                            :scope :street-segment
                            :identifier "VipObject.0.StreetSegment.4.City"
                            :error-type :missing})))
    (testing "city present is OK"
      (doseq [path ["VipObject.0.StreetSegment.1.City"
                    "VipObject.0.StreetSegment.2.City"
                    "VipObject.0.StreetSegment.3.City"]]
        (assert-no-problems errors
                            {:scope :street-segment
                             :identifier path
                             :error-type :missing})))))

(deftest ^:postgres validate-no-missing-state
  (let [errors-chan (a/chan 100)
        ctx {:xml-source-file-path (xml-input "v5-street-segment.xml")
             :errors-chan errors-chan}
        out-ctx (-> ctx
                    psql/start-run
                    xml/load-xml-ltree
                    v5.ss/validate-no-missing-state)
        errors (all-errors errors-chan)]
    (testing "state missing is an error"
      (is (contains-error? errors
                           {:severity :errors
                            :scope :street-segment
                            :identifier "VipObject.0.StreetSegment.0.State"
                            :error-type :missing}))
      (is (contains-error? errors
                           {:severity :errors
                            :scope :street-segment
                            :identifier "VipObject.0.StreetSegment.4.State"
                            :error-type :missing})))
    (testing "state present is OK"
      (doseq [path ["VipObject.0.StreetSegment.1.State"
                    "VipObject.0.StreetSegment.2.State"
                    "VipObject.0.StreetSegment.3.State"]]
        (assert-no-problems errors
                            {:scope :street-segment
                             :identifier path
                             :error-type :missing})))))

(deftest ^:postgres validate-no-missing-zip
  (let [errors-chan (a/chan 100)
        ctx {:xml-source-file-path (xml-input "v5-street-segment.xml")
             :errors-chan errors-chan}
        out-ctx (-> ctx
                    psql/start-run
                    xml/load-xml-ltree
                    v5.ss/validate-no-missing-zip)
        errors (all-errors errors-chan)]
    (testing "zip missing is an error"
      (is (contains-error? errors
                           {:severity :errors
                            :scope :street-segment
                            :identifier "VipObject.0.StreetSegment.0.Zip"
                            :error-type :missing}))
      (is (contains-error? errors
                           {:severity :errors
                            :scope :street-segment
                            :identifier "VipObject.0.StreetSegment.4.Zip"
                            :error-type :missing})))
    (testing "zip present is OK"
      (doseq [path ["VipObject.0.StreetSegment.1.Zip"
                    "VipObject.0.StreetSegment.2.Zip"
                    "VipObject.0.StreetSegment.3.Zip"]]
        (assert-no-problems errors
                            {:scope :street-segment
                             :identifier path
                             :error-type :missing})))))

(deftest ^:postgres validate-no-missing-street-name
  (let [errors-chan (a/chan 100)
        ctx {:xml-source-file-path (xml-input "v5-street-segment.xml")
             :errors-chan errors-chan}
        out-ctx (-> ctx
                    psql/start-run
                    xml/load-xml-ltree
                    v5.ss/validate-no-missing-street-name)
        errors (all-errors errors-chan)]
    (testing "street-name missing is an error"
      (is (contains-error? errors
                           {:severity :errors
                            :scope :street-segment
                            :identifier "VipObject.0.StreetSegment.0.StreetName"
                            :error-type :missing}))
      (is (contains-error? errors
                           {:severity :errors
                            :scope :street-segment
                            :identifier "VipObject.0.StreetSegment.4.StreetName"
                            :error-type :missing})))
    (testing "street-name present is OK"
      (doseq [path ["VipObject.0.StreetSegment.1.StreetName"
                    "VipObject.0.StreetSegment.2.StreetName"
                    "VipObject.0.StreetSegment.3.StreetName"]]
        (assert-no-problems errors
                            {:scope :street-segment
                             :identifier path
                             :error-type :missing})))))

(deftest ^:postgres validate-no-street-segment-overlaps-test
  (let [errors-chan (a/chan 100)
        ctx {:xml-source-file-path (xml-input "v5-street-segment-overlaps.xml")
             :errors-chan errors-chan}
        out-ctx (-> ctx
                    psql/start-run
                    xml/load-xml-ltree
                    xml.v5/load-xml-street-segments
                    v5.ss/validate-no-street-segment-overlaps)
        errors (all-errors errors-chan)]
    (are [_ path overlap]
        (if overlap
          (is (contains-error? errors
                               {:severity :errors
                                :scope :street-segment
                                :identifier path
                                :error-type :overlaps
                                :error-value overlap}))
          (assert-no-problems errors
                              {:severity :errors
                               :scope :street-segment
                               :identifier path
                               :error-type :overlaps}))
      "simple, complete overlap"    "VipObject.0.StreetSegment.0" "ss002"
      "partial overlap"             "VipObject.0.StreetSegment.2" "ss004"
      "single address overlap"      "VipObject.0.StreetSegment.4" "ss006"
      "even/odd don't overlap"      "VipObject.0.StreetSegment.6" nil
      "even/odd don't overlap"      "VipObject.0.StreetSegment.7" nil
      "even/both overlap"           "VipObject.0.StreetSegment.8" "ss010"
      "odd/both overlap"            "VipObject.0.StreetSegment.10" "ss012"
      "shared precinct"             "VipObject.0.StreetSegment.12" nil
      "shared precinct"             "VipObject.0.StreetSegment.13" nil
      "matching more fields"        "VipObject.0.StreetSegment.14" "ss016"
      "different address direction" "VipObject.0.StreetSegment.16" nil
      "different address direction" "VipObject.0.StreetSegment.17" nil
      "different street direction"  "VipObject.0.StreetSegment.18" nil
      "different street direction"  "VipObject.0.StreetSegment.19" nil
      "different street suffix"     "VipObject.0.StreetSegment.20" nil
      "different street suffix"     "VipObject.0.StreetSegment.21" nil)))

(deftest ^:postgres validate-start-house-number
  (let [errors-chan (a/chan 100)
        ctx {:xml-source-file-path (xml-input "v5-street-segment-start-house-number-invalid.xml")
             :errors-chan errors-chan}
        out-ctx (-> ctx
                    psql/start-run
                    xml/load-xml-ltree
                    v5.ss/validate-start-house-number)
        errors (all-errors errors-chan)]
    (testing "start house number invalid"
      (is (contains-error? errors
                           {:severity :fatal
                            :scope :street-segment
                            :identifier "VipObject.0.StreetSegment.2.StartHouseNumber.2"
                            :error-type :start-house-number
                            :error-value "1A"})))
    (testing "street-name present is OK"
      (doseq [path ["VipObject.0.StreetSegment.0.StreetName"
                    "VipObject.0.StreetSegment.1.StreetName"]]
        (assert-no-problems errors
                            {:scope :street-segment
                             :identifier path
                             :error-type :missing})))))

(deftest ^:postgres validate-end-house-number
  (let [errors-chan (a/chan 100)
        ctx {:xml-source-file-path (xml-input "v5-street-segment-end-house-number-invalid.xml")
             :errors-chan errors-chan}
        out-ctx (-> ctx
                    psql/start-run
                    xml/load-xml-ltree
                    v5.ss/validate-end-house-number)
        errors (all-errors errors-chan)]
    (testing "end house number invalid"
      (is (contains-error? errors
                           {:severity :fatal
                            :scope :street-segment
                            :identifier "VipObject.0.StreetSegment.2.EndHouseNumber.3"
                            :error-type :end-house-number
                            :error-value "100B"})))
    (testing "street-name present is OK"
      (doseq [path ["VipObject.0.StreetSegment.0.StreetName"
                    "VipObject.0.StreetSegment.1.StreetName"]]
        (assert-no-problems errors
                            {:scope :street-segment
                             :identifier path
                             :error-type :missing})))))

(deftest ^:postgres validate-house-number-prefix-no-includes-all-addresses
  (let [errors-chan (a/chan 100)
        ctx {:xml-source-file-path (xml-input "v5-street-segment-house-number-prefix.xml")
             :errors-chan errors-chan}
        out-ctx (-> ctx
                    psql/start-run
                    xml/load-xml-ltree
                    v5.ss/validate-no-includes-all-addresses-with-house-number-prefix)
        errors (all-errors errors-chan)]
    (testing "cannot includes all addresses with house number prefix"
      (is (contains-error? errors
                           {:severity :errors
                            :scope :street-segment
                            :identifier "VipObject.0.StreetSegment.1.HouseNumberPrefix"
                            :error-type :invalid
                            :error-value :invalid-house-number-prefix-with-includes-all-addresses})))
    (testing "Other segments are fine"
      (doseq [path ["VipObject.0.StreetSegment.0.HouseNumberPrefix"
                    "VipObject.0.StreetSegment.2.HouseNumberPrefix"]]
        (assert-no-problems errors
                            {:scope :street-segment
                             :identifier path
                             :error-type :invalid})))))

(deftest ^:postgres validate-house-number-prefix-no-includes-all-streets
  (let [errors-chan (a/chan 100)
        ctx {:xml-source-file-path (xml-input "v5-street-segment-house-number-prefix.xml")
             :errors-chan errors-chan}
        out-ctx (-> ctx
                    psql/start-run
                    xml/load-xml-ltree
                    v5.ss/validate-no-includes-all-streets-with-house-number-prefix)
        errors (all-errors errors-chan)]
    (testing "cannot includes all addresses with house number prefix"
      (is (contains-error? errors
                           {:severity :errors
                            :scope :street-segment
                            :identifier "VipObject.0.StreetSegment.2.HouseNumberPrefix"
                            :error-type :invalid
                            :error-value :invalid-house-number-prefix-with-includes-all-streets})))
    (testing "Other segments are fine"
      (doseq [path ["VipObject.0.StreetSegment.0.HouseNumberPrefix"
                    "VipObject.0.StreetSegment.1.HouseNumberPrefix"]]
        (assert-no-problems errors
                            {:scope :street-segment
                             :identifier path
                             :error-type :invalid})))))

(deftest ^:postgres validate-house-number-prefix-start-end-house-numbers
  (let [errors-chan (a/chan 100)
        ctx {:xml-source-file-path (xml-input "v5-street-segment-house-number-prefix.xml")
             :errors-chan errors-chan}
        out-ctx (-> ctx
                    psql/start-run
                    xml/load-xml-ltree
                    v5.ss/validate-start-end-house-number-with-house-number-prefix)
        errors (all-errors errors-chan)]
    (testing "cannot have different start/end house numbers when house number prefix is present"
      (is (contains-error? errors
                           {:severity :errors
                            :scope :street-segment
                            :identifier "VipObject.0.StreetSegment.0.HouseNumberPrefix"
                            :error-type :invalid
                            :error-value :start-and-end-house-numbers-must-be-identical-when-house-number-prefix-specified})))
    (testing "Other segments are fine"
      (doseq [path ["VipObject.0.StreetSegment.1.HouseNumberPrefix"
                    "VipObject.0.StreetSegment.2.HouseNumberPrefix"]]
        (assert-no-problems errors
                            {:scope :street-segment
                             :identifier path
                             :error-type :invalid})))))

(deftest ^:postgres validate-house-number-suffix-no-includes-all-addresses
  (let [errors-chan (a/chan 100)
        ctx {:xml-source-file-path (xml-input "v5-street-segment-house-number-suffix.xml")
             :errors-chan errors-chan}
        out-ctx (-> ctx
                    psql/start-run
                    xml/load-xml-ltree
                    v5.ss/validate-no-includes-all-addresses-with-house-number-suffix)
        errors (all-errors errors-chan)]
    (testing "cannot includes all addresses with house number suffix"
      (is (contains-error? errors
                           {:severity :errors
                            :scope :street-segment
                            :identifier "VipObject.0.StreetSegment.1.HouseNumberSuffix"
                            :error-type :invalid
                            :error-value :invalid-house-number-suffix-with-includes-all-addresses})))
    (testing "Other segments are fine"
      (doseq [path ["VipObject.0.StreetSegment.0.HouseNumberSuffix"
                    "VipObject.0.StreetSegment.2.HouseNumberSuffix"]]
        (assert-no-problems errors
                            {:scope :street-segment
                             :identifier path
                             :error-type :invalid})))))

(deftest ^:postgres validate-house-number-suffix-no-includes-all-streets
  (let [errors-chan (a/chan 100)
        ctx {:xml-source-file-path (xml-input "v5-street-segment-house-number-suffix.xml")
             :errors-chan errors-chan}
        out-ctx (-> ctx
                    psql/start-run
                    xml/load-xml-ltree
                    v5.ss/validate-no-includes-all-streets-with-house-number-suffix)
        errors (all-errors errors-chan)]
    (testing "cannot includes all streets with house number suffix"
      (is (contains-error? errors
                           {:severity :errors
                            :scope :street-segment
                            :identifier "VipObject.0.StreetSegment.2.HouseNumberSuffix"
                            :error-type :invalid
                            :error-value :invalid-house-number-suffix-with-includes-all-streets})))
    (testing "Other segments are fine"
      (doseq [path ["VipObject.0.StreetSegment.0.HouseNumberSuffix"
                    "VipObject.0.StreetSegment.1.HouseNumberSuffix"]]
        (assert-no-problems errors
                            {:scope :street-segment
                             :identifier path
                             :error-type :invalid})))))

(deftest ^:postgres validate-house-number-suffix-start-end-house-numbers
  (let [errors-chan (a/chan 100)
        ctx {:xml-source-file-path (xml-input "v5-street-segment-house-number-suffix.xml")
             :errors-chan errors-chan}
        out-ctx (-> ctx
                    psql/start-run
                    xml/load-xml-ltree
                    v5.ss/validate-start-end-house-number-with-house-number-suffix)
        errors (all-errors errors-chan)]
    (testing "cannot have different start/end house numbers when house number suffix is present"
      (is (contains-error? errors
                           {:severity :errors
                            :scope :street-segment
                            :identifier "VipObject.0.StreetSegment.0.HouseNumberSuffix"
                            :error-type :invalid
                            :error-value :start-and-end-house-numbers-must-be-identical-when-house-number-suffix-specified})))
    (testing "Other segments are fine"
      (doseq [path ["VipObject.0.StreetSegment.1.HouseNumberSuffix"
                    "VipObject.0.StreetSegment.2.HouseNumberSuffix"]]
        (assert-no-problems errors
                            {:scope :street-segment
                             :identifier path
                             :error-type :invalid})))))
