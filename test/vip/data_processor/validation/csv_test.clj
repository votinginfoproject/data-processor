(ns vip.data-processor.validation.csv-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.validation.csv :refer :all]
            [vip.data-processor.validation.data-spec.v3-0 :as v3-0]
            [vip.data-processor.db.sqlite :as sqlite]
            [korma.core :as korma])
  (:import [java.io File]))

(deftest remove-bad-filenames-test
  (let [bad-filenames [(File. "/data/BAD_FILE_NAME")
                       (File. "/data/BAD_FILE_NAME_2")]
        good-filenames (map #(File. (str "/data/" %)) (csv-filenames v3-0/data-specs))
        ctx {:input good-filenames
             :data-specs v3-0/data-specs}]
    (testing "with good filenames passes the context through"
      (is (= ctx (remove-bad-filenames ctx))))
    (testing "with bad filenames removes the bad files and warns"
      (let [ctx (update ctx :input (partial concat bad-filenames))
            out-ctx (remove-bad-filenames ctx)]
        (is (get-in out-ctx [:warnings :import :global :bad-filenames]))
        (is (not-every? (partial good-filename? v3-0/data-specs) (:input ctx)))
        (is (every? (partial good-filename? v3-0/data-specs) (:input out-ctx)))
        (assert-error-format out-ctx)))))

(deftest missing-files-test
  (testing "reports errors or warnings when certain files are missing"
    (let [ctx {:input (csv-inputs ["full-good-run/source.txt"])
               :data-specs v3-0/data-specs}
          out-ctx (-> ctx
                      ((error-on-missing-file "election.txt"))
                      ((error-on-missing-file "source.txt")))]
      (is (get-in out-ctx [:errors :elections :global :missing-csv]))
      (is (not (contains? (:errors out-ctx) :sources)))
      (assert-error-format out-ctx))))

(deftest csv-loader-test
  (testing "ignores unknown columns"
    (let [ctx (merge {:input (csv-inputs ["bad-columns/state.txt"])
                      :data-specs v3-0/data-specs}
                     (sqlite/temp-db "ignore-columns-test" "3.0"))
          out-ctx (load-csvs ctx)]
      (is (= [{:id 1 :name "NORTH CAROLINA" :election_administration_id 8}]
             (korma/select (get-in out-ctx [:tables :states]))))
      (testing "but warns about them"
        (is (get-in out-ctx [:warnings :states :global :extraneous-headers]))
        (assert-error-format out-ctx))))
  (testing "requires a header row"
    (let [ctx (merge {:input (csv-inputs ["no-header-row/ballot.txt"])
                      :data-specs v3-0/data-specs}
                     (sqlite/temp-db "no-headers-test" "3.0"))
          out-ctx (load-csvs ctx)]
      (is (= ["No header row"] (get-in out-ctx [:critical :ballots :global :no-header])))
      (assert-error-format out-ctx))))

(deftest missing-required-columns-test
  (let [ctx (merge {:input (csv-inputs ["missing-required-columns/contest.txt"])
                    :data-specs v3-0/data-specs}
                   (sqlite/temp-db "missing-required-columns" "3.0"))
        out-ctx (load-csvs ctx)]
    (testing "adds a critical error for contest.txt"
      (is (get-in out-ctx [:critical :contests :global :missing-headers]))
      (assert-error-format out-ctx))
    (testing "does not import contest.txt"
      (is (empty? (korma/select (get-in ctx [:tables :contests])))))))

(deftest report-bad-rows-test
  (let [ctx (merge {:input (csv-inputs ["bad-number-of-values/contest.txt"])
                    :data-specs v3-0/data-specs}
                   (sqlite/temp-db "bad-number-of-values" "3.0"))
        out-ctx (load-csvs ctx)]
    (testing "reports critical errors for rows with wrong number of values"
      (is (get-in out-ctx [:critical :contests 3 :number-of-values]))
      (is (get-in out-ctx [:critical :contests 5 :number-of-values]))
      (assert-error-format out-ctx))))

(deftest in-file-duplicate-ids-test
  (let [db (sqlite/temp-db "in-file-duplicate-ids" "3.0")
        ctx (merge {:input (csv-inputs ["in-file-duplicate-ids/contest.txt"])
                    :data-specs v3-0/data-specs}
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

(deftest byte-order-marker-test
  (let [db (sqlite/temp-db "byte-order-marker" "3.0")
        ctx (merge {:input (csv-inputs ["byte-order-marker/source.txt"])
                    :data-specs v3-0/data-specs}
                   db)
        out-ctx (load-csvs ctx)]
    (testing "loads successfully"
      (is (= ["Colorado Secretary of State"]
             (map :name
                  (korma/select
                   (get-in out-ctx [:tables :sources])
                   (korma/fields :name)))))
      (assert-error-format out-ctx))))

(deftest find-input-file-test
  (let [election-file (File. "/data/election.txt")
        state-file    (File. "/data/state.txt")
        upper-case-source (File. "/data/Source.txt")
        ctx {:input [election-file state-file upper-case-source]}]
    (testing "finds files from their name"
      (is (= election-file (find-input-file ctx "election.txt")))
      (is (= state-file    (find-input-file ctx "state.txt"))))
    (testing "returns nil if not found"
      (is (nil? (find-input-file ctx "DOES_NOT_EXIST.txt"))))
    (testing "finds files without regard to the file's case"
      (is (= upper-case-source (find-input-file ctx "source.txt"))))))

(deftest low-number-vip-id-test
  (let [db (sqlite/temp-db "low-number-vip-id-test" "3.0")
        ctx (merge {:input (csv-inputs ["low-number-vip-id/source.txt"])
                    :data-specs v3-0/data-specs}
                   db)
        out-ctx (load-csvs ctx)]
    (testing "does not mangle the vip_id"
      (is (= ["01"]
             (map :vip_id
                  (korma/select
                   (get-in out-ctx [:tables :sources])
                   (korma/fields :vip_id)))))
      (assert-error-format out-ctx))))
