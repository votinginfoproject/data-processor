(ns vip.data-processor.pipelines
  (:require [vip.data-processor.pipelines.csv.v3 :as csv.v3]
            [vip.data-processor.pipelines.csv.v5 :as csv.v5]
            [vip.data-processor.pipelines.xml.v3 :as xml.v3]
            [vip.data-processor.pipelines.xml.v5 :as xml.v5]))

(defn choose-pipeline [ctx]
  (cond
    (and (= :csv (:format ctx))
         (= "3.0" (:feed-family ctx)))
    (update ctx :pipeline (partial concat csv.v3/pipeline))

    (and (= :csv (:format ctx))
         (= "5.1" (:feed-family ctx)))
    (update ctx :pipeline (partial concat csv.v5/pipeline))

    (and (= :xml (:format ctx))
         (= "3.0" (:feed-family ctx)))
    (update ctx :pipeline (partial concat xml.v3/pipeline))

    (and (= :xml (:format ctx))
         (= "5.1" (:feed-family ctx)))
    (update ctx :pipeline (partial concat xml.v5/pipeline))

    :else
    (throw (ex-info "No pipeline matching format and feed-family version"
                    (select-keys ctx [:format :feed-family])))))
