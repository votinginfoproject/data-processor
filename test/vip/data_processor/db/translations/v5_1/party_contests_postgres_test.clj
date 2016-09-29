(ns vip.data-processor.db.translations.v5-1.party-contests-postgres-test
  (:require [clojure.test :refer :all]
            [vip.data-processor.validation.csv :as csv]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.test-helpers :refer :all]
            [vip.data-processor.validation.data-spec :as data-spec]
            [vip.data-processor.validation.data-spec.v5-1 :as v5-1]
            [clojure.core.async :as a]))

(use-fixtures :once setup-postgres)

(deftest ^:postgres transformer-test
  (testing "party_contest.txt can be loaded and transformed"
    (let [errors-chan (a/chan 100)
          ctx {:input (csv-inputs ["5-1/party_contest.txt"])
               :errors-chan errors-chan
               :spec-version (atom "5.1")
               :pipeline (concat
                          [postgres/start-run
                           (data-spec/add-data-specs v5-1/data-specs)]
                          (get csv/version-pipelines "5.1"))}
          out-ctx (pipeline/run-pipeline ctx)
          errors (all-errors errors-chan)]
      (assert-no-problems errors {})
      (are-xml-tree-values
       out-ctx
       "pcon001" "VipObject.0.PartyContest.0.id"
       "Sit or stand" "VipObject.0.PartyContest.0.BallotTitle.3.Text.0"
       "en" "VipObject.0.PartyContest.0.BallotTitle.3.Text.0.language"
       "other" "VipObject.0.PartyContest.1.ExternalIdentifiers.5.ExternalIdentifier.0.Type.0"
       "stubbornness" "VipObject.0.PartyContest.1.ExternalIdentifiers.5.ExternalIdentifier.0.OtherType.1"
       "allez!" "VipObject.0.PartyContest.1.ExternalIdentifiers.5.ExternalIdentifier.0.Value.2"))))
