(ns vip.data-processor.validation.xml-with-postgres-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.validation.data-spec.v3-0 :as v3-0]
            [vip.data-processor.validation.db :as db]
            [vip.data-processor.validation.xml :refer :all]
            [vip.data-processor.validation.v5.email :as v5.email]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.errors.process :as process]
            [vip.data-processor.db.postgres :as psql]
            [vip.data-processor.db.sqlite :as sqlite]
            [squishy.data-readers]
            [korma.core :as korma]
            [clojure.core.async :as a]))

(use-fixtures :once setup-postgres)

(deftest ^:postgres load-xml-ltree-test
  (testing "imports xml values into postgres"
    (let [ctx {:xml-source-file-path (xml-input "v5_sample_feed.xml")
               :pipeline [psql/start-run
                          load-xml-ltree]}
          out-ctx (pipeline/run-pipeline ctx)]
      (is (= (-> (korma/select psql/xml-tree-values
                   (korma/where {:results_id (:import-id out-ctx)})
                   (korma/where (psql/ltree-match psql/xml-tree-values
                                                  :path
                                                  "VipObject.0.Source.*{1}.VipId.*{1}")))
                 first
                 :value)
             "51")))))

(deftest ^:postgres validate-emails-test
  (testing "adds errors to the context for badly formatted emails"
    (let [errors-chan (a/chan 100)
          ctx {:xml-source-file-path (xml-input "v5-bad-emails.xml")
               :errors-chan errors-chan
               :pipeline [psql/start-run
                          load-xml-ltree
                          v5.email/validate-emails]}
          out-ctx (pipeline/run-pipeline ctx)
          errors (all-errors errors-chan)]
      (is (contains-error? errors
                           {:severity :errors
                            :scope :email
                            :identifier "VipObject.0.Person.0.ContactInformation.0.Email.0"
                            :error-type :format}))
      (is (contains-error? errors
                           {:severity :errors
                            :scope :email
                            :identifier "VipObject.0.Person.1.ContactInformation.0.Email.0"
                            :error-type :format}))
      (testing "but not for good emails"
        (assert-no-problems errors
                            {:severity :errors
                             :scope :email
                             :identifier "VipObject.0.Person.2.ContactInformation.0.Email.0"
                             :error-type :format})))))

(deftest ^:postgres full-good-run-test
  (testing "a good XML file produces no erorrs or warnings"
    (let [errors-chan (a/chan 100)
          ctx (merge {:xml-source-file-path (xml-input "full-good-run.xml")
                      :format :xml
                      :data-specs v3-0/data-specs
                      :errors-chan errors-chan
                      :spec-version nil
                      :spec-family nil
                      :pipeline (concat [psql/start-run
                                         determine-spec-version
                                         sqlite/attach-sqlite-db
                                         process/process-v3-validations
                                         load-xml]
                                        db/validations)}
                     (sqlite/temp-db "full-good-xml" "3.0"))
          out-ctx (pipeline/run-pipeline ctx)
          errors (all-errors errors-chan)]
      (is (nil? (:stop out-ctx)))
      (is (nil? (:exception out-ctx)))
      (assert-no-problems errors {})
      (testing "inserts values for columns not in the first element of a type"
        (let [mail-only-precinct (first
                                  (korma/select (get-in out-ctx [:tables :precincts])
                                                (korma/where {:id 10203})))]
          (is (= 1 (:mail_only mail-only-precinct))))))))
