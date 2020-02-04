(ns vip.data-processor.validation.csv-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.validation.csv :refer :all]
            [vip.data-processor.validation.data-spec.v3-0 :as v3-0]
            [vip.data-processor.db.sqlite :as sqlite]
            [korma.core :as korma]
            [clojure.core.async :as a])
  (:import [java.io File]
           [java.nio.file Paths]))

(deftest remove-bad-filenames-test
  (let [data-dir (Paths/get "/data" (into-array String []))
        bad-filenames [(.resolve data-dir "BAD_FILE_NAME")
                       (.resolve data-dir "BAD_FILE_NAME_2")]
        good-filenames (map #(.resolve data-dir (:filename %)) v3-0/data-specs)
        errors-chan (a/chan 100)
        ctx {:csv-source-file-paths good-filenames
             :errors-chan errors-chan
             :spec-version (atom "3.0")
             :data-specs v3-0/data-specs}]
    (testing "with good filenames passes the context through"
      (is (= ctx (remove-bad-filenames ctx))))
    (testing "with bad filenames removes the bad files and warns"
      (let [ctx (update ctx :csv-source-file-paths (partial concat bad-filenames))
            out-ctx (remove-bad-filenames ctx)
            errors (all-errors errors-chan)]
        (is (contains-error? errors
                             {:severity :warnings
                              :scope :import
                              :identifier :global
                              :error-type :bad-filenames}))
        (is (not-every? (partial good-filename? v3-0/data-specs)
                        (:csv-source-file-paths ctx)))
        (is (every? (partial good-filename? v3-0/data-specs)
                    (:csv-source-file-paths out-ctx)))))))

(deftest error-on-missing-files-test
  (testing "reports errors or warnings when certain files are missing"
    (let [errors-chan (a/chan 100)
          ctx {:csv-source-file-paths (csv-inputs ["full-good-run/source.txt"])
               :errors-chan errors-chan
               :data-specs v3-0/data-specs}
          out-ctx (-> ctx
                      error-on-missing-files)
          errors (all-errors errors-chan)]
      (is (contains-error? errors
                           {:severity :errors
                            :scope :elections
                            :identifier :global
                            :error-type :missing-csv}))

      ;; required, present
      (is (not (contains-error? errors
                                {:serverity :errors
                                 :scope :sources})))
      ;; not required, missing
      (is (not (contains-error? errors
                                {:serverity :errors
                                 :scope :contests}))))))

(deftest csv-loader-test
  (testing "ignores unknown columns"
    (let [errors-chan (a/chan 100)
          ctx (merge {:csv-source-file-paths (csv-inputs ["bad-columns/state.txt"])
                      :errors-chan errors-chan
                      :data-specs v3-0/data-specs}
                     (sqlite/temp-db "ignore-columns-test" "3.0"))
          out-ctx (load-csvs ctx)
          errors (all-errors errors-chan)]
      (is (= [{:id 1 :name "NORTH CAROLINA" :election_administration_id 8}]
             (korma/select (get-in out-ctx [:tables :states]))))
      (testing "but warns about them"
        (is (contains-error? errors
                             {:severity :warnings
                              :scope :states
                              :identifier :global
                              :error-type :extraneous-headers})))))
  (testing "requires a header row"
    (let [errors-chan (a/chan 100)
          ctx (merge {:csv-source-file-paths (csv-inputs ["no-header-row/ballot.txt"])
                      :errors-chan errors-chan
                      :data-specs v3-0/data-specs}
                     (sqlite/temp-db "no-headers-test" "3.0"))
          out-ctx (load-csvs ctx)
          errors (all-errors errors-chan)]
      (is (contains-error? errors
                           {:severity :critical
                            :scope :ballots
                            :identifier :global
                            :error-type :no-header
                            :error-value "No header row"})))))

(deftest missing-required-columns-test
  (let [errors-chan (a/chan 100)
        ctx (merge {:csv-source-file-paths (csv-inputs ["missing-required-columns/contest.txt"])
                    :errors-chan errors-chan
                    :data-specs (filter #(= :contests (:table %)) v3-0/data-specs)}
                   (sqlite/temp-db "missing-required-columns" "3.0"))
        out-ctx (load-csvs ctx)
        errors (all-errors errors-chan)]
    (testing "adds a critical error for contest.txt"
      (is (contains-error? errors
                           {:severity :critical
                            :scope :contests
                            :identifier :global
                            :error-type :missing-headers})))
    (testing "does not import contest.txt"
      (is (empty? (korma/select (get-in ctx [:tables :contests])))))))

(deftest report-bad-rows-test
  (let [errors-chan (a/chan 100)
        ctx (merge {:csv-source-file-paths (csv-inputs ["bad-number-of-values/contest.txt"])
                    :errors-chan errors-chan
                    :spec-version (atom "3.0")
                    :data-specs v3-0/data-specs}
                   (sqlite/temp-db "bad-number-of-values" "3.0"))
        out-ctx (load-csvs ctx)
        errors (all-errors errors-chan)]
    (testing "reports critical errors for rows with wrong number of values"
      (is (contains-error? errors
                           {:severity :critical
                            :scope :contests
                            :identifier 3
                            :error-type :number-of-values}))
      (is (contains-error? errors
                           {:severity :critical
                            :scope :contests
                            :identifier 5
                            :error-type :number-of-values})))))

(deftest in-file-duplicate-ids-test
  (let [errors-chan (a/chan 100)
        db (sqlite/temp-db "in-file-duplicate-ids" "3.0")
        ctx (merge {:csv-source-file-paths (csv-inputs ["in-file-duplicate-ids/contest.txt"])
                    :errors-chan errors-chan
                    :data-specs v3-0/data-specs}
                   db)]
    (korma/insert (get-in db [:tables :contests])
                  (korma/values {:id 6}))
    (let [out-ctx (load-csvs ctx)
          errors (all-errors errors-chan)]
      (testing "reports fatal errors for duplicate ids"
        (is (contains-error? errors
                             {:severity :fatal
                              :scope :contests
                              :identifier "3"
                              :error-type :duplicate-ids}))
        (is (contains-error? errors
                             {:severity :fatal
                              :scope :contests
                              :identifier "5"
                              :error-type :duplicate-ids}))
        (is (contains-error? errors
                             {:severity :fatal
                              :scope :contests
                              :identifier "6"
                              :error-type :duplicate-ids}))
        (is (= [1 2 4 6]
               (map :id
                    (korma/select (get-in db [:tables :contests])
                                  (korma/fields :id)
                                  (korma/order :id :ASC)))))))))

(deftest byte-order-marker-test
  (let [db (sqlite/temp-db "byte-order-marker" "3.0")
        ctx (merge {:csv-source-file-paths (csv-inputs ["byte-order-marker/source.txt"])
                    :data-specs v3-0/data-specs}
                   db)
        out-ctx (load-csvs ctx)]
    (testing "loads successfully"
      (is (= ["Colorado Secretary of State"]
             (map :name
                  (korma/select
                   (get-in out-ctx [:tables :sources])
                   (korma/fields :name))))))))

(deftest low-number-vip-id-test
  (let [db (sqlite/temp-db "low-number-vip-id-test" "3.0")
        ctx (merge {:csv-source-file-paths (csv-inputs ["low-number-vip-id/source.txt"])
                    :data-specs v3-0/data-specs}
                   db)
        out-ctx (load-csvs ctx)]
    (testing "does not mangle the vip_id"
      (is (= ["01"]
             (map :vip_id
                  (korma/select
                   (get-in out-ctx [:tables :sources])
                   (korma/fields :vip_id))))))))

(deftest determine-spec-version-test
  (testing "finds and assocs the version of the csv feed for 3.0 files"
    (let [ctx {:csv-source-file-paths (csv-inputs ["full-good-run/source.txt"])
               :spec-version (atom nil)}
          out-ctx (determine-spec-version ctx)]
      (is (= "3.0" @(get out-ctx :spec-version)))))

  (testing "finds and assocs the version of the csv feed for 5.1 files"
    (let [ctx {:csv-source-file-paths (csv-inputs ["5-1/spec-version/source.txt"])
               :spec-version (atom nil)}
          out-ctx (determine-spec-version ctx)]
      (is (= "5.1" @(get out-ctx :spec-version))))))
