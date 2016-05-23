(ns vip.data-processor.test-helpers
  (:require [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [clojure.java.io :as io]
            [clojure.java.jdbc :as jdbc]
            [korma.core :as korma]
            [korma.db :as db]
            [turbovote.resource-config :refer [config]]
            [vip.data-processor.db.postgres :as psql]
            [vip.data-processor.util :as util]))

(set! *print-length* 10)

(def problem-types [:warnings :errors :critical :fatal])

(defn assert-no-problems
  "Test that there are no errors of any level for the key-path below
  that level.

    (assert-no-problems ctx [:missing :ballot])

  That will check there is nothing
  at [:warnings :missing :ballot], [:errors :missing :ballot], etc."
  [ctx key-path]
  (doseq [problem-type problem-types]
    (is (empty? (get-in ctx (cons problem-type key-path))))))

(defn assert-some-problem
  "Test that there is an error of some level for the key-path."
  [ctx key-path]
  (is (seq (remove nil? (map #(get-in ctx (cons % key-path)) problem-types)))))

(defn csv-inputs [file-names]
  (map #(->> %
             (str "csv/")
             io/resource
             io/as-file)
       file-names))

(defn xml-input [file-name]
  [(->> file-name
         (str "xml/")
         io/resource
         io/as-file)])

(defn assert-column [ctx table column values]
  (is (= values
         (map column
              (korma/select (get-in ctx [:tables table])
                            (korma/fields column)
                            (korma/order :id :ASC))))))

(defn assert-error-format
  [out-ctx]
  (doseq [severity problem-types]
    (if-let [errors (get out-ctx severity)]
      (let [flattened (util/flatten-keys errors)]
        (doseq [[path-to-errors errors] flattened]
          (is (keyword? (first path-to-errors)))
          (is (= 3 (count path-to-errors)))
          (is (sequential? errors))
          (is (not (empty? errors))))))))

(defonce setup-postgres-has-run (atom false))

(defn setup-postgres
  "A :once test fixture to drop and create the test database. This can only be
  run once due to an undiagnosed connection leakage"
  [f]
  (when-not @setup-postgres-has-run
    (log/info "Recreating the test database")
    (let [database-name (config [:postgres :database])
          jdbc-config {:dbtype "postgresql"
                       :dbname "postgres"
                       :host (config [:postgres :host])
                       :port (config [:postgres :port])
                       :user (config [:postgres :user])
                       :password (config [:postgres :password])}]

      (jdbc/execute! jdbc-config [(str "DROP DATABASE IF EXISTS " database-name)] :transaction? false)
      (jdbc/execute! jdbc-config [(str "CREATE DATABASE " database-name)] :transaction? false)

      (psql/initialize)
      ;; these vars will be unbound until after psql/initialize, so
      ;; don't set psql-tables until after that's been run
      (def psql-tables (concat (vals psql/v5-1-tables)
                               [psql/v5-1-street-segments
                                psql/xml-tree-validations
                                psql/xml-tree-values
                                psql/election_approvals
                                psql/statistics
                                psql/validations
                                psql/results]))
      (reset! setup-postgres-has-run true)))
  (f))

(defn with-clean-postgres
  "An :each test fixture to clear out tables in the test database. "
  [f]
  (doseq [table psql-tables]
    (korma/delete table))
  (f))
