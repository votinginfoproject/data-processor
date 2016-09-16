(ns vip.data-processor.output.tree-xml-postgres-test
  (:require [clojure.test :refer :all]
            [korma.core :as korma]
            [vip.data-processor.db.postgres :as postgres]
            [vip.data-processor.output.tree-xml :refer :all]
            [vip.data-processor.pipeline :as pipeline]
            [vip.data-processor.test-helpers :refer :all]
            [clojure.core.async :as a]))

(use-fixtures :once setup-postgres)

(deftest ^:postgres pipeline-test
  (let [import-id (-> postgres/results
                      (korma/insert
                       (korma/values {:public_id (name (gensym))}))
                      :id)
        _ (korma/insert postgres/xml-tree-values
            (korma/values
             [{:results_id import-id
               :path (postgres/path->ltree "VipObject.0.Candidate.0.id")
               :simple_path (postgres/path->ltree "VipObject.Candidate.id")
               :parent_with_id (postgres/path->ltree "VipObject.0.Candidate.0.id")
               :value "can001"}
              {:results_id import-id
               :path (postgres/path->ltree "VipObject.0.Candidate.0.Name.0")
               :simple_path (postgres/path->ltree "VipObject.Candidate.Name")
               :parent_with_id (postgres/path->ltree "VipObject.0.Candidate.0.id")
               :value "Frank"}
              {:results_id import-id
               :path (postgres/path->ltree "VipObject.0.Candidate.0.Party.1")
               :simple_path (postgres/path->ltree "VipObject.Candidate.Party")
               :parent_with_id (postgres/path->ltree "VipObject.0.Candidate.0.id")
               :value "Every day"}
              {:results_id import-id
               :path (postgres/path->ltree "VipObject.0.Candidate.0.Title.2.Text.0.language")
               :simple_path (postgres/path->ltree "VipObject.Candidate.Title.Text.language")
               :parent_with_id (postgres/path->ltree "VipObject.0.Candidate.0.id")
               :value "en"}
              {:results_id import-id
               :path (postgres/path->ltree "VipObject.0.Candidate.0.Title.2.Text.0")
               :simple_path (postgres/path->ltree "VipObject.Candidate.Title.Text")
               :parent_with_id (postgres/path->ltree "VipObject.0.Candidate.0.id")
               :value "President"}
              {:results_id import-id
               :path (postgres/path->ltree "VipObject.0.Candidate.0.Title.2.Text.1.language")
               :simple_path (postgres/path->ltree "VipObject.Candidate.Title.Text.language")
               :parent_with_id (postgres/path->ltree "VipObject.0.Candidate.0.id")
               :value "es"}
              {:results_id import-id
               :path (postgres/path->ltree "VipObject.0.Candidate.0.Title.2.Text.1")
               :simple_path (postgres/path->ltree "VipObject.Candidate.Title.Text")
               :parent_with_id (postgres/path->ltree "VipObject.0.Candidate.0.id")
               :value "\"El\" Presidente"}
              {:results_id import-id
               :path (postgres/path->ltree "VipObject.0.Candidate.0.Nickname.3")
               :simple_path (postgres/path->ltree "VipObject.Candidate.Nickname")
               :parent_with_id (postgres/path->ltree "VipObject.0.Candidate.0.id")
               :value "> Ezra"}
              {:results_id import-id
               :path (postgres/path->ltree "VipObject.0.Contest.1.id")
               :simple_path (postgres/path->ltree "VipObject.Contest.id")
               :parent_with_id (postgres/path->ltree "VipObject.0.Contest.1.id")
               :value "con001"}]))
        errors-chan (a/chan 100)
        ctx {:spec-version (atom "5.1.2")
             :errors-chan errors-chan
             :import-id import-id
             :pipeline pipeline}
        out-ctx (pipeline/run-pipeline ctx)
        errors (all-errors errors-chan)]
    (assert-no-problems-2 errors {})
    (is (= (-> out-ctx
               :xml-output-file
               .toFile
               slurp)
           "<?xml version=\"1.0\"?>\n<VipObject xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" schemaVersion=\"5.1.2\" xsi:noNamespaceSchemaLocation=\"http://votinginfoproject.github.com/vip-specification/vip_spec.xsd\">\n<Candidate id=\"can001\"><Name>Frank</Name><Party>Every day</Party><Title><Text language=\"en\">President</Text><Text language=\"es\">&quot;El&quot; Presidente</Text></Title><Nickname>&gt; Ezra</Nickname></Candidate><Contest id=\"con001\"></Contest></VipObject>"))))
