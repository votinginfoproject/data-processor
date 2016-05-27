(ns vip.data-processor.db.translations.v5-1.party-contests-postgres-test
  (:require [clojure.test :refer :all]
            [korma.core :as korma]
            [vip.data-processor.validation.csv :as csv]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.db.translations.v5-1.party-contests :as pc]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.test-helpers :refer :all]))

(use-fixtures :once setup-postgres)

(deftest ^:postgres transformer-test
  (testing "party_contest.txt can be loaded and transformed"
    (let [ctx {:input (csv-inputs ["5-1/party_contest.txt"])
               :spec-version "5.1"
               :ltree-index 0
               :pipeline (concat
                          [postgres/start-run]
                          (get csv/version-pipelines "5.1")
                          [pc/transformer])}
          out-ctx (pipeline/run-pipeline ctx)]
      (assert-no-problems out-ctx [])
      (are-xml-tree-values out-ctx
                           "pcon001" "VipObject.0.PartyContest.0.id"
                           "Sit or stand" "VipObject.0.PartyContest.0.BallotTitle.3.Text.0"
                           "en" "VipObject.0.PartyContest.0.BallotTitle.3.Text.0.language"
                           "other" "VipObject.0.PartyContest.1.ExternalIdentifiers.5.ExternalIdentifier.0.Type.0"
                           "stubbornness" "VipObject.0.PartyContest.1.ExternalIdentifiers.5.ExternalIdentifier.0.OtherType.1"
                           "allez!" "VipObject.0.PartyContest.1.ExternalIdentifiers.5.ExternalIdentifier.0.Value.2"))))
