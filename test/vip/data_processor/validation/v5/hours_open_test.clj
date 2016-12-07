(ns vip.data-processor.validation.v5.hours-open-test
  (:require [vip.data-processor.validation.v5.hours-open :as v5.hours-open]
            [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.db.postgres :as psql]
            [vip.data-processor.validation.xml :as xml]
            [clojure.core.async :as a]))

(use-fixtures :once setup-postgres)

(deftest ^:postgres validate-dates-test
  (let [errors-chan (a/chan 100)
        ctx {:input (xml-input "v5-hours-open.xml")
             :errors-chan errors-chan}
        out-ctx (-> ctx
                    psql/start-run
                    xml/load-xml-ltree
                    v5.hours-open/validate-dates)
        errors (all-errors errors-chan)]

    (testing "valid start and end dates produce no errors"
      (assert-no-problems errors
                          {:severity :errors
                           :scope :hours-open
                           :identifier "VipObject.0.HoursOpen.0.Schedule.0.StartDate.1"})

      (assert-no-problems errors
                          {:severity :errors
                           :scope :hours-open
                           :identifier "VipObject.0.HoursOpen.0.Schedule.0.EndDate.2"}))

    (testing "a date that looks like a timestamp is invalid"
      (is (contains-error? errors
                           {:severity :errors
                            :scope :hours-open
                            :identifier "VipObject.0.HoursOpen.1.Schedule.0.StartDate.1"
                            :error-type :format})))

    (testing "a leap day in the wrong year is invalid"
      (is (contains-error? errors
                           {:severity :errors
                            :scope :hours-open
                            :identifier "VipObject.0.HoursOpen.1.Schedule.0.EndDate.2"
                            :error-type :format})))))

(deftest ^:postgres validate-times-test
  (let [errors-chan (a/chan 100)
        ctx {:input (xml-input "v5-hours-open.xml")
             :errors-chan errors-chan}
        out-ctx (-> ctx
                    psql/start-run
                    xml/load-xml-ltree
                    v5.hours-open/validate-times)
        errors (all-errors errors-chan)]
    (testing "StartTime and EndTime valid produces no errors"
      (assert-no-problems errors
                          {:severity :errors
                           :scope :hours-open
                           :identifier "VipObject.0.HoursOpen.0.Schedule.0.Hours.0.StartTime.0"})
      (assert-no-problems errors
                          {:severity :errors
                           :scope :hours-open
                           :identifier "VipObject.0.HoursOpen.0.Schedule.0.Hours.0.EndTime.1"}))
    (testing "StartTime hours out of range produces one error"
      (is (contains-error? errors
                           {:severity :errors
                            :scope :hours-open
                            :identifier "VipObject.0.HoursOpen.1.Schedule.0.Hours.0.StartTime.0"
                            :error-type :format}))
      (assert-no-problems errors
                          {:severity :errors
                           :scope :hours-open
                           :identifier "VipObject.0.HoursOpen.1.Schedule.0.Hours.0.EndTime.1"}))
    (testing "StartTime missing time zone produces one error"
      (is (contains-error? errors
                           {:severity :errors
                            :scope :hours-open
                            :identifier "VipObject.0.HoursOpen.2.Schedule.0.Hours.0.StartTime.0"
                            :error-type :format}))
      (assert-no-problems errors
                          {:severity :errors
                           :scope :hours-open
                           :identifier "VipObject.0.HoursOpen.2.Schedule.0.Hours.0.EndTime.1"}))
    (testing "EndTime invalid produces one error"
      (assert-no-problems errors
                          {:severity :errors
                           :scope :hours-open
                           :identifier "VipObject.0.HoursOpen.3.Schedule.0.Hours.0.StartTime.0"})
      (is (contains-error? errors
                           {:severity :errors
                            :scope :hours-open
                            :identifier "VipObject.0.HoursOpen.3.Schedule.0.Hours.0.EndTime.1"
                            :error-type :format})))
    (testing "StartTime and EndTime invalid produces two errors"
      (is (contains-error? errors
                           {:severity :errors
                            :scope :hours-open
                            :identifier "VipObject.0.HoursOpen.4.Schedule.0.Hours.0.StartTime.0"
                            :error-type :format}))
      (is (contains-error? errors
                           {:severity :errors
                            :scope :hours-open
                            :identifier "VipObject.0.HoursOpen.4.Schedule.0.Hours.0.EndTime.1"
                            :error-type :format})))))
