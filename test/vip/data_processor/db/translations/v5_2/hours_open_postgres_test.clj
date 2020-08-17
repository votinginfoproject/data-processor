(ns vip.data-processor.db.translations.v5-2.hours-open-postgres-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.db.translations.v5-2.hours-open :as ho]
            [vip.data-processor.errors.process :as process]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.validation.csv :as csv]
            [vip.data-processor.validation.data-spec :as data-spec]
            [vip.data-processor.validation.data-spec.v5-2 :as v5-2]
            [clojure.core.async :as a]))

(use-fixtures :once setup-postgres)

(deftest ^:postgres hours-open->ltree-entries-test
  (testing "schedule.txt is loaded and transformed into HoursOpen entities"
    (let [errors-chan (a/chan 100)
          ctx {:csv-source-file-paths (csv-inputs ["5-2/schedule.txt"])
               :errors-chan errors-chan
               :spec-version "5.2"
               :spec-family "5.2"
               :pipeline [postgres/start-run
                          (data-spec/add-data-specs v5-2/data-specs)
                          postgres/prep-v5-2-run
                          process/process-v5-validations
                          csv/load-csvs
                          ho/transformer]}
          out-ctx (pipeline/run-pipeline ctx)
          errors (all-errors errors-chan)]
      (assert-no-problems errors {})
      (testing "multiple Schedules for one HoursOpen"
        (are-xml-tree-values out-ctx
         "ho001" "VipObject.0.HoursOpen.0.id"
         "sch001" "VipObject.0.HoursOpen.0.Schedule.0.label"
         "07:00:00-06:00" "VipObject.0.HoursOpen.0.Schedule.0.Hours.0.StartTime.0"
         "22:00:00-06:00" "VipObject.0.HoursOpen.0.Schedule.0.Hours.0.EndTime.1"
         "true" "VipObject.0.HoursOpen.0.Schedule.0.IsOrByAppointment.1"
         "2016-10-10" "VipObject.0.HoursOpen.0.Schedule.0.StartDate.2"
         "2016-10-12" "VipObject.0.HoursOpen.0.Schedule.0.EndDate.3"

         "sch002" "VipObject.0.HoursOpen.0.Schedule.1.label"
         "09:00:00-06:00" "VipObject.0.HoursOpen.0.Schedule.1.Hours.0.StartTime.0"
         "20:00:00-06:00" "VipObject.0.HoursOpen.0.Schedule.1.Hours.0.EndTime.1"
         "true" "VipObject.0.HoursOpen.0.Schedule.1.IsOnlyByAppointment.1"
         "2016-10-13" "VipObject.0.HoursOpen.0.Schedule.1.StartDate.2"
         "2016-10-15" "VipObject.0.HoursOpen.0.Schedule.1.EndDate.3"))
      (testing "another HoursOpen from a third row"
        (are-xml-tree-values out-ctx
         "ho002" "VipObject.0.HoursOpen.1.id"
         "sch003" "VipObject.0.HoursOpen.1.Schedule.0.label"
         "08:00:00-06:00" "VipObject.0.HoursOpen.1.Schedule.0.Hours.0.StartTime.0"
         "14:00:00-06:00" "VipObject.0.HoursOpen.1.Schedule.0.Hours.0.EndTime.1"
         "true" "VipObject.0.HoursOpen.1.Schedule.0.IsSubjectToChange.1"
         "2016-10-10" "VipObject.0.HoursOpen.1.Schedule.0.StartDate.2"
         "2016-10-15" "VipObject.0.HoursOpen.1.Schedule.0.EndDate.3")))))
