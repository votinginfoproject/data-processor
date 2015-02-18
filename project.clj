(defproject vip.data-processor "0.1.0-SNAPSHOT"
  :description "Voting Information Project Data Processor"
  :dependencies [[org.clojure/clojure "1.7.0-alpha5"]
                 [org.slf4j/slf4j-log4j12 "1.7.10"]
                 [org.clojure/tools.logging "0.3.1"]
                 [com.novemberain/langohr "3.0.1"]
                 [joda-time "2.7"]
                 [clj-aws-s3 "0.3.10" :exclusions [joda-time]]
                 [democracyworks.squishy "1.0.0" :exclusions [joda-time
                                                              org.slf4j/slf4j-simple]]
                 [net.lingala.zip4j/zip4j "1.3.2"]
                 [turbovote.resource-config "0.1.4"]
                 [joplin.jdbc "0.2.7"]
                 [korma "0.4.0"]
                 [org.clojure/java.jdbc "0.3.5"]
                 [org.postgresql/postgresql "9.4-1200-jdbc4" :exclusions [org.slf4j/slf4j-simple]]
                 [org.xerial/sqlite-jdbc "3.8.7"]]
  :profiles {:test {:resource-paths ["test-resources"]}}
  :main vip.data-processor)
