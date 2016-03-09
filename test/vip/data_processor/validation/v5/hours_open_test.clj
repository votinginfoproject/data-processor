(ns vip.data-processor.validation.v5.hours-open-test
  (:require [vip.data-processor.validation.v5.hours-open :as v5.hours-open]
            [clojure.test :refer :all]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.db.postgres :as psql]
            [vip.data-processor.validation.xml :as xml]))

(use-fixtures :once setup-postgres)
(use-fixtures :each with-clean-postgres)

(deftest ^:postgres validate-times-test
  (let [ctx {:input (xml-input "v5-hours-open.xml")}
        out-ctx (-> ctx
                    psql/start-run
                    xml/load-xml-ltree
                    v5.hours-open/validate-times)]
    (testing "StartTime and EndTime valid produces no errors"
      (is (not (get-in out-ctx [:errors :hours-open
                                "VipObject.0.HoursOpen.0.Schedule.0.Hours.0.StartTime.0"])))
      (is (not (get-in out-ctx [:errors :hours-open
                                "VipObject.0.HoursOpen.0.Schedule.0.Hours.0.EndTime.1"]))))
    (testing "StartTime hours out of range produces one error"
      (is (get-in out-ctx [:errors :hours-open
                           "VipObject.0.HoursOpen.1.Schedule.0.Hours.0.StartTime.0"
                           :format]))
      (is (not (get-in out-ctx [:errors :hours-open
                                "VipObject.0.HoursOpen.1.Schedule.0.Hours.0.EndTime.1"]))))
    (testing "StartTime missing time zone produces one error"
      (is (get-in out-ctx [:errors :hours-open
                           "VipObject.0.HoursOpen.2.Schedule.0.Hours.0.StartTime.0"
                           :format]))
      (is (not (get-in out-ctx [:errors :hours-open
                                "VipObject.0.HoursOpen.2.Schedule.0.Hours.0.EndTime.1"]))))
    (testing "EndTime invalid produces one error"
      (is (not (get-in out-ctx [:errors :hours-open
                                "VipObject.0.HoursOpen.3.Schedule.0.Hours.0.StartTime.0"])))
      (is (get-in out-ctx [:errors :hours-open
                           "VipObject.0.HoursOpen.3.Schedule.0.Hours.0.EndTime.1"
                           :format])))
    (testing "StartTime and EndTime invalid produces two errors"
      (is (get-in out-ctx [:errors :hours-open
                           "VipObject.0.HoursOpen.4.Schedule.0.Hours.0.StartTime.0"
                           :format]))
      (is (get-in out-ctx [:errors :hours-open
                           "VipObject.0.HoursOpen.4.Schedule.0.Hours.0.EndTime.1"
                           :format])))))
