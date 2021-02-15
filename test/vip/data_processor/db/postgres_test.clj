(ns vip.data-processor.db.postgres-test
  (:require [vip.data-processor.db.postgres :refer :all]
            [turbovote.resource-config :refer [config]]
            [clojure.test :refer :all]))

(deftest url-test
  (let [db-spec-1 {:host "host"
                   :port "5678"
                   :user "us&r"
                   :password "p&ssw@rd"
                   :database "database"}]
    (with-redefs [config (constantly db-spec-1)]
      (is (= "jdbc:postgresql://host:5678/database?user=us%26r&password=p%26ssw%40rd"
             (url))
          "Percent encoding is done for username and password values"))))

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
    (is (= "invalid-4" (build-public-id "" nil "" 4))))

  (testing "most punctuation and any non-ascii characters are stripped"
    (is (= "1969-06-20-a-space-race-pal-11" (build-public-id "6/20/1969" "a space" "race, pal!" 11)))
    (is (= "2017-01-05-jalapenos-42" (build-public-id "1/5/2017" "jalapeños" nil 42)))))

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

(deftest election-id-test
  (testing "election-id generation"
   (is (= "2015-10-10-LOUISIANA-GENERAL"
          (build-election-id "2015-10-10" "LOUISIANA" "GENERAL")))
   (is (= "2015-10-10-LOUISIANA-GENERAL"
          (build-election-id "2015-10-10" "LOUISIANA   " "GENERAL")))
   (is (nil? (build-election-id "2015-10-10" "LOUISIANA" nil)))
   (is (nil? (build-election-id "2015-10-10" "" "GENERAL")))))

(deftest v5-summary-branch-test
  (let [v3-ctx {:pipeline []
                :spec-version "3.0"
                :spec-family "3.0"}
        v5-ctx {:pipeline []
                :spec-version "5.1.2"
                :spec-family "5.2"}
        v3-out-ctx (v5-summary-branch v3-ctx)
        v5-out-ctx (v5-summary-branch v5-ctx)]
    (is (= (count (v3-out-ctx :pipeline)) 0))
    (is (= (count (v5-out-ctx :pipeline)) 4))))
