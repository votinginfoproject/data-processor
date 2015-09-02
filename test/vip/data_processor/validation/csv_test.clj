(ns vip.data-processor.validation.csv-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.validation.csv :refer :all]
            [vip.data-processor.validation.data-spec :refer [data-specs]]
            [vip.data-processor.db.sqlite :as sqlite]
            [korma.core :as korma])
  (:import [java.io File]))

(deftest remove-bad-filenames-test
  (let [bad-filenames [(File. "/data/BAD_FILE_NAME")
                       (File. "/data/BAD_FILE_NAME_2")]
        good-filenames (map #(File. (str "/data/" %)) csv-filenames)
        ctx {:input good-filenames}]
    (testing "with good filenames passes the context through"
      (is (= ctx (remove-bad-filenames ctx))))
    (testing "with bad filenames removes the bad files and warns"
      (let [ctx (update ctx :input (partial concat bad-filenames))
            out-ctx (remove-bad-filenames ctx)]
        (is (get-in out-ctx [:warnings :import :global :bad-filenames]))
        (is (not-every? good-filename? (:input ctx)))
        (is (every? good-filename? (:input out-ctx)))
        (assert-error-format out-ctx)))))

(deftest missing-files-test
  (testing "reports errors or warnings when certain files are missing"
    (let [ctx {:input (csv-inputs ["full-good-run/source.txt"])}
          out-ctx (-> ctx
                      ((error-on-missing-file "election.txt"))
                      ((error-on-missing-file "source.txt"))
                      ((warn-on-missing-file "state.txt")))]
      (is (get-in out-ctx [:errors :elections :global :missing-csv]))
      (is (get-in out-ctx [:warnings :states :global :missing-csv]))
      (is (not (contains? (:errors out-ctx) :sources)))
      (assert-error-format out-ctx))))

(deftest csv-loader-test
  (testing "ignores unknown columns"
    (let [ctx (merge {:input (csv-inputs ["bad-columns/state.txt"])
                      :data-specs data-specs}
                     (sqlite/temp-db "ignore-columns-test"))
          out-ctx (load-csvs ctx)]
      (is (= [{:id 1 :name "NORTH CAROLINA" :election_administration_id 8}]
             (korma/select (get-in out-ctx [:tables :states]))))
      (testing "but warns about them"
        (is (get-in out-ctx [:warnings :states :global :extraneous-headers]))
        (assert-error-format out-ctx))))
  (testing "requires a header row"
    (let [ctx (merge {:input (csv-inputs ["no-header-row/ballot.txt"])
                      :data-specs data-specs}
                     (sqlite/temp-db "no-headers-test"))
          out-ctx (load-csvs ctx)]
      (is (= ["No header row"] (get-in out-ctx [:critical :ballots :global :no-header])))
      (assert-error-format out-ctx))))

(deftest missing-required-columns-test
  (let [ctx (merge {:input (csv-inputs ["missing-required-columns/contest.txt"])
                    :data-specs data-specs}
                   (sqlite/temp-db "missing-required-columns"))
        out-ctx (load-csvs ctx)]
    (testing "adds a critical error for contest.txt"
      (is (get-in out-ctx [:critical :contests :global :missing-headers]))
      (assert-error-format out-ctx))
    (testing "does not import contest.txt"
      (is (empty? (korma/select (get-in ctx [:tables :contests])))))))

(deftest report-bad-rows-test
  (let [ctx (merge {:input (csv-inputs ["bad-number-of-values/contest.txt"])
                    :data-specs data-specs}
                   (sqlite/temp-db "bad-number-of-values"))
        out-ctx (load-csvs ctx)]
    (testing "reports critical errors for rows with wrong number of values"
      (is (get-in out-ctx [:critical :contests 3 :number-of-values]))
      (is (get-in out-ctx [:critical :contests 5 :number-of-values]))
      (assert-error-format out-ctx))))

(deftest in-file-duplicate-ids-test
  (let [db (sqlite/temp-db "in-file-duplicate-ids")
        ctx (merge {:input (csv-inputs ["in-file-duplicate-ids/contest.txt"])
                    :data-specs data-specs}
                   db)]
    (korma/insert (get-in db [:tables :contests])
                  (korma/values {:id 6}))
    (let [out-ctx (load-csvs ctx)]
      (testing "reports fatal errors for duplicate ids"
        (is (get-in out-ctx [:fatal :contests "3" :duplicate-ids]))
        (is (get-in out-ctx [:fatal :contests "5" :duplicate-ids]))
        (is (get-in out-ctx [:fatal :contests "6" :duplicate-ids]))
        (is (= [1 2 4 6]
               (map :id
                    (korma/select (get-in db [:tables :contests])
                                  (korma/fields :id)
                                  (korma/order :id :ASC)))))
        (assert-error-format out-ctx)))))
