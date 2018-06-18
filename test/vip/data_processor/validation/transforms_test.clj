(ns vip.data-processor.validation.transforms-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.validation.csv :as csv]
            [vip.data-processor.validation.data-spec :as data-spec]
            [vip.data-processor.validation.data-spec.v3-0 :as v3-0]
            [vip.data-processor.validation.db :as db]
            [vip.data-processor.validation.transforms :refer :all]
            [vip.data-processor.db.sqlite :as sqlite]
            [clojure.java.io :as io]
            [korma.core :as korma]
            [clojure.core.async :as a])
  (:import [java.io File]))

(deftest csv-validations-test
  (testing "full run on good files"
    (let [db (sqlite/temp-db "good-run-test" "3.0")
          filenames (->> v3-0/data-specs
                         (map
                          #(io/as-file (io/resource (str "csv/full-good-run/" (:filename %)))))
                         (remove nil?))
          errors-chan (a/chan 100)
          ctx (merge {:input filenames
                      :errors-chan errors-chan
                      :spec-version (atom nil)
                      :pipeline (concat [(data-spec/add-data-specs v3-0/data-specs)]
                                        csv-validations
                                        db/validations)} db)
          results-ctx (pipeline/run-pipeline ctx)
          errors (all-errors errors-chan)]
      (is (nil? (:stop results-ctx)))
      (is (nil? (:exception results-ctx)))
      (assert-no-problems errors {}))))

(deftest remove-invalid-extensions-test
  (testing "removes non-csv, txt, or xml files from :input"
    (let [errors-chan (a/chan 100)
          ctx {:input [(File. "good-file.xml")
                       (File. "not-so-good-file.xls")
                       (File. "logo.ai")]
               :errors-chan errors-chan}
          results-ctx (remove-invalid-extensions ctx)
          errors (all-errors errors-chan)]
      (is (= 1 (count (:input results-ctx))))
      (is (= "good-file.xml" (-> results-ctx :input first .getName)))
      (is (contains-error? errors
                           {:severity :warnings
                            :scope :import
                            :identifier :global
                            :error-type :invalid-extensions
                            :error-value '("not-so-good-file.xls" "logo.ai")}))))
  (testing "allows uppercase file extensions"
    (let [errors-chan (a/chan 100)
          ctx {:input [(File. "this-is-okay.XML")
                       (File. "so-is-this.TXT")
                       (File. "but-not-this.NAIL")]
               :errors-chan errors-chan}
          results-ctx (remove-invalid-extensions ctx)
          errors (all-errors errors-chan)]
      (is (= [(File. "this-is-okay.XML")
              (File. "so-is-this.TXT")]
             (:input results-ctx)))
      (is (contains-error? errors
                           {:severity :warnings
                            :scope :import
                            :identifier :global
                            :error-type :invalid-extensions
                            :error-value '("but-not-this.NAIL")})))))
