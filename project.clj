(defproject vip.data-processor "0.1.0-SNAPSHOT"
  :description "Voting Information Project Data Processor"
  :dependencies [[org.clojure/clojure "1.7.0-alpha5"]
                 [org.slf4j/slf4j-log4j12 "1.7.10"]
                 [org.clojure/tools.logging "0.3.1"]
                 [joda-time "2.7"]
                 [clj-aws-s3 "0.3.10" :exclusions [joda-time]]
                 [democracyworks.squishy "1.0.0" :exclusions [joda-time
                                                              org.slf4j/slf4j-simple]]
                 [turbovote.resource-config "0.1.3"]]
  :profiles {:test {:resource-paths ["test-resources"]}}
  :main vip.data-processor)
