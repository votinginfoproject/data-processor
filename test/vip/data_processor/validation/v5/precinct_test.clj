(ns vip.data-processor.validation.v5.precinct-test
  (:require  [clojure.test :refer :all]
             [vip.data-processor.pipeline :as pipeline]
             [vip.data-processor.db.postgres :as psql]
             [vip.data-processor.validation.xml :as xml]
             [vip.data-processor.test-helpers :refer :all]
             [vip.data-processor.validation.v5.precinct :as precinct]))

(use-fixtures :once setup-postgres)
(use-fixtures :each with-clean-postgres)

(deftest ^:postgres validate-no-missing-names-test
  (let [ctx {:input (xml-input "v5-precincts.xml")
             :pipeline [psql/start-run
                        xml/load-xml-ltree
                        precinct/validate-no-missing-names]}
        out-ctx (pipeline/run-pipeline ctx)]
    (testing "missing Names are flagged"
      (is (get-in out-ctx [:errors :precincts "VipObject.0.Precinct.0.Name" :missing]))
      (is (get-in out-ctx [:errors :precincts "VipObject.0.Precinct.1.Name" :missing])))
    (testing "doesn't for those that aren't"
      (is (not (get-in out-ctx [:errors :precincts "VipObject.0.Precinct.2.Name" :missing])))
      (is (not (get-in out-ctx [:errors :precincts "VipObject.0.Precinct.3.Name" :missing]))))))

(deftest ^:postgres validate-no-missing-locality-ids-test
  (let [ctx {:input (xml-input "v5-precincts.xml")
             :pipeline [psql/start-run
                        xml/load-xml-ltree
                        precinct/validate-no-missing-locality-ids]}
        out-ctx (pipeline/run-pipeline ctx)]
    (testing "missing LocalityIds are flagged"
      (is (get-in out-ctx [:errors :precincts "VipObject.0.Precinct.0.LocalityId" :missing]))
      (is (get-in out-ctx [:errors :precincts "VipObject.0.Precinct.2.LocalityId" :missing])))
    (testing "doesn't for those that aren't"
      (is (not (get-in out-ctx [:errors :precincts "VipObject.0.Precinct.1.LocalityId" :missing])))
      (is (not (get-in out-ctx [:errors :precincts "VipObject.0.Precinct.3.LocalityId" :missing]))))))
