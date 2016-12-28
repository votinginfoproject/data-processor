(ns vip.data-processor.validation.v5.locality-test
  (:require [vip.data-processor.validation.v5.locality :as v5.locality]
            [clojure.test :refer :all]
            [korma.core :as korma]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor :as data-processor]
            [vip.data-processor.db.postgres :as psql]
            [vip.data-processor.errors :as errors]
            [vip.data-processor.errors.process :as process]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.validation.transforms :as t]
            [vip.data-processor.validation.v5 :as v5]
            [vip.data-processor.validation.xml :as xml]
            [clojure.core.async :as a]))

(use-fixtures :once setup-postgres)

(deftest ^:postgres validate-no-missing-names-test
  (let [errors-chan (a/chan 100)
        ctx {:input (xml-input "v5-localities.xml")
             :errors-chan errors-chan}
        out-ctx (-> ctx
                    psql/start-run
                    xml/load-xml-ltree
                    v5.locality/validate-no-missing-names)
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
                    v5.locality/validate-no-missing-state-ids)
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

(deftest ^:postgres locality-summary-test
  (let [errors-chan (a/chan 100)
        pipeline [psql/start-run
                        t/xml-csv-branch
                        data-processor/add-validations
                        errors/close-errors-chan
                        errors/await-statistics]
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
