(ns vip.data-processor.validation.v6.external-file-test
  (:require  [clojure.test :refer :all]
             [vip.data-processor.pipeline :as pipeline]
             [vip.data-processor.db.postgres :as psql]
             [vip.data-processor.validation.xml :as xml]
             [vip.data-processor.test-helpers :refer :all]
             [vip.data-processor.validation.v6.external-file :as v6.external-file]
             [clojure.core.async :as a]))

(use-fixtures :once setup-postgres)

(deftest ^:postgres validate-no-missing-file-names-success-test
  (let [errors-chan (a/chan 100)
        ctx {:errors-chan errors-chan
             :xml-source-file-path (xml-input "v6_sample_feed.xml")
             :pipeline [psql/start-run
                        xml/load-xml-ltree
                        v6.external-file/validate-no-missing-file-names]}
        out-ctx (pipeline/run-pipeline ctx)
        errors (all-errors errors-chan)]
    (are [path]
        (assert-no-problems errors
                            {:severity :errors
                             :scope :external-file
                             :identifier path})
      "VipObject.0.ExternalFile.3.Filename")))

(deftest ^:postgres validate-no-missing-file-names-failure-test
  (let [errors-chan (a/chan 100)
        ctx {:errors-chan errors-chan
             :xml-source-file-path (xml-input "v6-external-file-without-name.xml")
             :pipeline [psql/start-run
                        xml/load-xml-ltree
                        v6.external-file/validate-no-missing-file-names]}
        out-ctx (pipeline/run-pipeline ctx)
        errors (all-errors errors-chan)]
    (testing "missing Filenames are flagged"
      (is (contains-error? errors
                           {:severity :errors
                            :scope :external-files
                            :identifier "VipObject.0.ExternalFile.0.Filename"
                            :error-type :missing}))
      (is (contains-error? errors
                           {:severity :errors
                            :scope :external-files
                            :identifier "VipObject.0.ExternalFile.1.Filename"
                            :error-type :missing}))
      (is (not (contains-error? errors
                                {:severity :errors
                                 :scope :external-files
                                 :identifier "VipObject.0.ExternalFile.2.Filename"
                                 :error-type :missing}))))))
