(ns vip.data-processor.db.translations.v5-1.schedules
  (:require [vip.data-processor.db.translations.util :as util]
            [clojure.string :as str]))

(defn hours->ltree [parent-path parent-with-id idx-fn schedule]
  (when-not (and (str/blank? (:start_time schedule))
                 (str/blank? (:end_time schedule)))
    (let [path (str parent-path ".Hours." (idx-fn))
          child-idx-fn (util/index-generator 0)]
      (mapcat #(% child-idx-fn path schedule)
              [(util/simple-value->ltree :start_time
                                         "StartTime"
                                         parent-with-id)
               (util/simple-value->ltree :end_time
                                         "EndTime"
                                         parent-with-id)]))))

(defn schedule->ltree [parent-path idx-fn schedule]
  (let [path (str parent-path ".Schedule." (idx-fn))
        label-path (str path ".label")
        parent-with-id (util/id-path parent-path)
        child-idx-fn (util/index-generator 0)]
    (concat
     (list
      {:path label-path
       :simple_path (util/path->simple-path label-path)
       :parent_with_id parent-with-id
       :value (:id schedule)})
     (hours->ltree path parent-with-id child-idx-fn schedule)
     (mapcat #(% child-idx-fn path schedule)
             [(util/simple-value->ltree :is_only_by_appointment
                                        "IsOnlyByAppointment"
                                        parent-with-id)
              (util/simple-value->ltree :is_or_by_appointment
                                        "IsOrByAppointment"
                                        parent-with-id)
              (util/simple-value->ltree :is_subject_to_change
                                        "IsSubjectToChange"
                                        parent-with-id)
              (util/simple-value->ltree :start_date
                                        "StartDate"
                                        parent-with-id)
              (util/simple-value->ltree :end_date
                                        "EndDate"
                                        parent-with-id)]))))
