(ns vip.data-processor.validation.v5.locality-test
  (:require [vip.data-processor.validation.v5.locality :as locality]
            [clojure.test :refer :all]
            [korma.core :as korma]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor :as data-processor]
            [vip.data-processor.db.postgres :as psql]
            [vip.data-processor.errors :as errors]
            [vip.data-processor.errors.process :as process]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.validation.transforms :as t]
            [vip.data-processor.validation.xml :as xml]
            [vip.data-processor.validation.v5 :as v5]
            [clojure.core.async :as a]))

(use-fixtures :once setup-postgres)

(deftest ^:postgres validate-no-missing-names-test
  (let [errors-chan (a/chan 100)
        ctx {:input (xml-input "v5-localities.xml")
             :errors-chan errors-chan}
        out-ctx (-> ctx
                    psql/start-run
                    xml/load-xml-ltree
                    locality/validate-no-missing-names)
        errors (all-errors errors-chan)]
    (testing "name missing is an error"
      (is (contains-error? errors
                           {:severity :errors
                            :scope :locality
                            :identifier "VipObject.0.Locality.0.Name"
                            :error-type :missing})))
    (testing "name present is OK"
      (assert-no-problems errors
                          {:severity :errors
                           :scope :locality
                           :identifier "VipObject.0.Locality.1.Name"
                           :error-type :missing}))))

(deftest ^:postgres validate-no-missing-state-ids-test
  (let [errors-chan (a/chan 100)
        ctx {:input (xml-input "v5-localities.xml")
             :errors-chan errors-chan}
        out-ctx (-> ctx
                    psql/start-run
                    xml/load-xml-ltree
                    locality/validate-no-missing-state-ids)
        errors (all-errors errors-chan)]
    (testing "state-id missing is an error"
      (is (contains-error? errors
                           {:severity :errors
                            :scope :locality
                            :identifier "VipObject.0.Locality.0.StateId"
                            :error-type :missing})))
    (testing "state-id present is OK"
      (assert-no-problems errors
                          {:severity :errors
                           :scope :locality
                           :identifier "VipObject.0.Locality.1.StateId"
                           :error-type :missing}))))

(deftest ^:postgres populating-paths-by-locality
  (let [errors-chan (a/chan 100)
        pipeline (concat
                  [psql/start-run
                   process/process-v5-validations
                   xml/load-xml-ltree
                   psql/populate-locality-table]
                  v5/validations
                  [errors/close-errors-chan
                   errors/await-statistics])
        ctx {:input (xml-input "v5-locality-summaries.xml")
             :spec-version (atom "5.1.2")
             :errors-chan errors-chan
             :pipeline pipeline}

        out-ctx (pipeline/run-pipeline ctx)
        results-id (:import-id out-ctx)]
    (testing "we have three localities on the paths_by_locality table"
      (let [localities (-> (korma/select psql/v5-dashboard-paths-by-locality
                             (korma/aggregate (count :*) :count)
                             (korma/where {:results_id results-id}))
                           first)]
        (is (= 3 (:count localities)))))
    (testing "we have 13 paths for loc3 on paths_by_locality"
      (let [pathstring (-> (korma/select psql/v5-dashboard-paths-by-locality
                             (korma/fields :paths)
                             (korma/where {:locality_id "loc3" :results_id results-id}))
                           first :paths)
            paths (-> pathstring (clojure.string/replace #"[\{\}]" " ") (clojure.string/split #",") set)]
        (is (= 13 (count paths)))))))

(deftest ^:postgres locality-error-report
  (let [errors-chan (a/chan 100)
        pipeline (concat
                  [psql/start-run
                   process/process-v5-validations
                   xml/load-xml-ltree
                   psql/populate-locality-table]
                  v5/validations
                  [errors/close-errors-chan
                   errors/await-statistics])
        ctx {:input (xml-input "v5-locality-summaries.xml")
             :spec-version (atom "5.1.2")
             :errors-chan errors-chan
             :pipeline pipeline}
        out-ctx (pipeline/run-pipeline ctx)
        results-id (:import-id out-ctx)]
    (testing "we get the correct number of errors in a locality error report"
      (let
        [public-id (-> (korma/select psql/results
                         (korma/fields :public_id)
                         (korma/where {:id results-id}))
                      first
                      :public_id)
         report (-> (korma/exec-raw
                     [(str "select * from v5_dashboard.locality_error_report('" public-id "', 'loc3')")]
                     :results)
                    set)]
        (is (= 3 (count report)))
        (is (contains?
             report
             {:results_id results-id
              :path "VipObject.0.StreetSegment.31.Zip"
              :severity "errors"
              :scope "street-segment"
              :identifier "ss33"
              :error_type "missing"
              :error_data ":missing-zip"}))))))

(deftest ^:postgres locality-summary-test
  (let [errors-chan (a/chan 100)
        pipeline (concat
                  [psql/start-run
                   process/process-v5-validations
                   xml/load-xml-ltree
                   psql/populate-locality-table]
                  v5/validations
                  [errors/close-errors-chan
                   errors/await-statistics])
        ctx {:input (xml-input "v5-locality-summaries.xml")
             :spec-version (atom "5.1.2")
             :errors-chan errors-chan
             :pipeline pipeline}

        out-ctx (pipeline/run-pipeline ctx)
        errors (all-errors errors-chan)
        results-id (:import-id out-ctx)]

    (testing "we have three localities"
      (let [localities (-> (korma/select psql/v5-dashboard-localities
                             (korma/aggregate (count :*) :count)
                             (korma/where {:results_id results-id}))
                           first)]
        (is (= 3 (:count localities)))))

    (testing "a locality has some basic info"
      (let [localities (-> (korma/select psql/v5-dashboard-localities
                             (korma/fields :name :type :id)
                             (korma/where {:results_id results-id})))]
        (is (= #{{:name "Salida"
                  :type "city"
                  :id "loc3"}
                 {:name "Nathrop"
                  :type "ghost town"
                  :id "loc2"}
                 {:name "Buena Vista"
                  :type "town"
                  :id "loc1"}}
               (set localities)))))

    (testing "in a locality, we count elements, their errors, and success rates"
      (let [salida (-> (korma/select psql/v5-dashboard-localities
                         (korma/where {:id "loc3" :results_id results-id}))
                       first)]
        (is (= {:id "loc3"
                :name "Salida"
                :type "city"
                :error_count 4
                :precinct_errors 0
                :precinct_count 1
                :precinct_completion 100
                :polling_location_errors 0
                :polling_location_count 2
                :polling_location_completion 100
                :street_segment_errors 1
                :street_segment_count 3
                :street_segment_completion 66
                :hours_open_errors 2
                :hours_open_count 1
                :hours_open_completion 0
                :election_administration_errors 0
                :election_administration_count 1
                :election_administration_completion 100
                :department_errors 0
                :department_count 4
                :department_completion 100
                :voter_service_errors 1
                :voter_service_count 1
                :voter_service_completion 0
                :results_id results-id}
               salida))))))
