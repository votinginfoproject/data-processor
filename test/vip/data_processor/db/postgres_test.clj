(ns vip.data-processor.db.postgres-test
  (:require [vip.data-processor.db.postgres :refer :all]
            [clojure.test :refer :all]))

(deftest build-public-id-test
  (testing "builds public ids with as much information as it has"
    (is (= "2015-06-18-federal-Ohio-4" (build-public-id "2015-06-18" "federal" "Ohio" 4)))
    (is (= "federal-Ohio-4" (build-public-id nil "federal" "Ohio" 4)))
    (is (= "2015-06-18-Ohio-4" (build-public-id "2015-06-18" nil "Ohio" 4)))
    (is (= "2015-06-18-federal-4" (build-public-id "2015-06-18" "federal" nil 4)))
    (is (= "2015-06-18-4" (build-public-id "2015-06-18" nil nil 4)))
    (is (= "federal-4" (build-public-id nil "federal" nil 4)))
    (is (= "Ohio-4" (build-public-id nil nil "Ohio" 4)))
    (is (= "2015-06-18-federal-Ohio-4" (build-public-id "2015-06-18" "federal" "Ohio" 4)))
    (is (= "federal-Ohio-4" (build-public-id "" "federal" "Ohio" 4)))
    (is (= "2015-06-18-Ohio-4" (build-public-id "2015-06-18" "" "Ohio" 4)))
    (is (= "2015-06-18-federal-4" (build-public-id "2015-06-18" "federal" "" 4)))
    (is (= "2015-06-18-4" (build-public-id "2015-06-18" "" "" 4)))
    (is (= "federal-4" (build-public-id "" "federal" "" 4)))
    (is (= "Ohio-4" (build-public-id "" "" "Ohio" 4)))
    (is (= "2015-06-18-federal-Ohio-4" (build-public-id "6/18/2015" "federal" "Ohio" 4)))
    (is (= "federal-Ohio-4" (build-public-id "//////" "federal" "Ohio" 4)))
    (is (= "federal-Ohio-4" (build-public-id "" "federal " "Ohio " 4))))
  (testing "gives an 'invalid' named id if all of date, election-type and state are nil"
    (is (= "invalid-4" (build-public-id nil nil nil 4)))
    (is (= "invalid-4" (build-public-id "" nil "" 4)))))

(deftest coerce-identifier-test
  (testing "coerces valid identifiers"
    (is (= global-identifier (coerce-identifier :global)))
    (is (= (BigDecimal. 4) (coerce-identifier "4")))
    (is (= 5 (coerce-identifier 5)))
    (is (nil? (coerce-identifier nil))))
  (testing "uses the invalid-identifier when given an invalid identifier"
    (is (= invalid-identifier (coerce-identifier :garbage)))
    (is (= invalid-identifier (coerce-identifier '(a list))))
    (is (= invalid-identifier (coerce-identifier "ABC")))))

(deftest validation-values-test
  (testing "generates validation values for all kinds of errors"
    (let [ctx {:import-id 34
               :warnings {:ballots {:global {:missing-headers ["name" "id"]}
                                    3 {:bad-format ["school must match /\\w+/"]
                                       :too-long ["less than 140 characters"]}}
                          :election-results {nil {:unreferenced-ids [12 45]}}}
               :errors {:sources {:global {:number-of-rows ["sources.txt must have one row"]}}}}
          values (validation-values ctx)]
      (is (= 7 (count values)))
      (is (every? #(= 34 (:results_id %)) values))
      (testing "creates a validation value for each error-data"
        (let [missing-header-values (filter #(= "missing-headers" (:error_type %)) values)]
          (is (= 2 (count missing-header-values)))
          (is (apply = (map #(dissoc % :error_data) missing-header-values))))
        (is (= 1 (->> values
                    (filter #(= "bad-format" (:error_type %)))
                    count)))))))

(deftest xml-tree-validation-values-test
  (testing "generates validates values for 5.1 style errors"
    (let [ctx {:import-id 25
               :errors {}
               :fatal {:id
                       {"VipObject.0.Election.1.id"
                        {:duplicate ["ele0001"]}
                        "VipObject.0.Election.2.id"
                        {:duplicate ["ele0001"]}}}
               :warnings {:import
                          {:global
                           {:invalid-extensions [".exemell" ".seeesvee"]}}}}
          values (xml-tree-validation-values ctx)]
      (is (= 4 (count values)))
      (is (= 2
             (->> values
                  (filter #(= "duplicate" (:error_type %)))
                  count))))))

(deftest election-id-test
  (testing "election-id generation"
   (is (= "2015-10-10-LOUISIANA-GENERAL"
          (build-election-id "2015-10-10" "LOUISIANA" "GENERAL")))
   (is (= "2015-10-10-LOUISIANA-GENERAL"
          (build-election-id "2015-10-10" "LOUISIANA   " "GENERAL")))
   (is (nil? (build-election-id "2015-10-10" "LOUISIANA" nil)))
   (is (nil? (build-election-id "2015-10-10" "" "GENERAL")))))
