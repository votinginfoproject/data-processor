(defproject vip.data-processor "0.1.0-SNAPSHOT"
  :description "Voting Information Project Data Processor"
  :dependencies [[org.clojure/clojure "1.7.0-alpha5"]
                 [org.clojure/tools.logging "0.3.1"]
                 [joda-time "2.7"]
                 [clj-aws-s3 "0.3.10" :exclusions [joda-time]]
                 [democracyworks.squishy "1.0.0" :exclusions [joda-time]]
                 [turbovote.resource-config "0.1.3"]]
  :main vip.data-processor)
