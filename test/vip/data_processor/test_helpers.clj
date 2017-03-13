(ns vip.data-processor.test-helpers
  (:require [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [clojure.java.io :as io]
            [clojure.java.jdbc :as jdbc]
            [korma.core :as korma]
            [korma.db :as db]
            [turbovote.resource-config :refer [config]]
            [vip.data-processor.db.postgres :as psql]
            [vip.data-processor.util :as util]
            [clojure.core.async :as a]))

(def problem-types [:warnings :errors :critical :fatal])

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

      (jdbc/execute! jdbc-config [(str "DROP DATABASE IF EXISTS " database-name)] {:transaction? false})
      (jdbc/execute! jdbc-config [(str "CREATE DATABASE " database-name)] {:transaction? false})

      (psql/initialize)
      ;; these vars will be unbound until after psql/initialize, so
      ;; don't set psql-tables until after that's been run
      (def psql-tables (concat (vals psql/v5-1-tables)
                               [psql/v5-1-street-segments
                                psql/xml-tree-validations
                                psql/xml-tree-values
                                psql/election_approvals
                                psql/statistics
                                psql/v5-statistics
                                psql/validations
                                psql/results]))
      (reset! setup-postgres-has-run true)))
  (f))

(defmacro are-xml-tree-values
  [out-ctx & args]
  `(are [value# path#] (= value#
                          (->
                           (korma/select psql/xml-tree-values
                             (korma/fields :value)
                             (korma/where {:results_id (:import-id ~out-ctx)
                                           :path (psql/path->ltree path#)}))
                           first
                           :value))
     ~@args))

(defn all-errors [errors-chan]
  (a/<!! (a/timeout 100)) ;; give a moment
  (a/close! errors-chan)
  (a/<!!
   (a/into [] errors-chan)))

(defn matching-errors [errors error]
  (let [ks (keys error)]
    (filter #(= error
                (select-keys % ks))
            errors)))

(defn contains-error? [errors error]
  (seq (matching-errors errors error)))

(defn assert-no-problems [errors error]
  (is (not (contains-error? errors error))))
