(defproject vip.data-processor "0.1.0-SNAPSHOT"
  :description "Voting Information Project Data Processor"
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.apache.logging.log4j/log4j-core "2.5"]
                 [org.apache.logging.log4j/log4j-slf4j-impl "2.5"]
                 [org.slf4j/slf4j-api "1.7.20"]
                 [org.clojure/data.csv "0.1.3"]
                 [org.clojure/data.xml "0.0.8"]
                 [org.clojure/tools.logging "0.3.1"]
                 [org.apache.directory.studio/org.apache.commons.lang "2.6"]
                 [com.novemberain/langohr "3.5.1"]
                 [joda-time "2.9.3"]
                 [clj-aws-s3 "0.3.10" :exclusions [joda-time]]
                 [com.climate/clj-newrelic "0.2.1"]
                 [democracyworks/squishy "2.0.0" :exclusions [joda-time
                                                              org.slf4j/slf4j-simple
                                                              org.slf4j/slf4j-api]]
                 [net.lingala.zip4j/zip4j "1.3.2"]
                 [turbovote.resource-config "0.2.0"]
                 [joplin.jdbc "0.3.6"]
                 [korma "0.4.2"]
                 [org.clojure/java.jdbc "0.5.0"]
                 [org.postgresql/postgresql "9.4.1208" :exclusions [org.slf4j/slf4j-simple
                                                                    org.slf4j/slf4j-api]]
                 [org.xerial/sqlite-jdbc "3.8.11.2"]]
  :plugins [[com.carouselapps/jar-copier "0.3.0"]]
  :profiles {:test {:resource-paths ["test-resources"]
                    :dependencies [[com.github.kyleburton/clj-xpath "1.4.5"]]}
             :dev {:source-paths ["dev-src"]
                   :resource-paths ["dev-resources"]
                   :main dev.core}}
  :test-selectors {:default (complement :postgres)
                   :postgres :postgres
                   :all (constantly true)}
  :java-agents [[com.newrelic.agent.java/newrelic-agent "3.20.0"]]
  :jar-copier {:java-agents true
               :destination "resources/jars"}
  :prep-tasks ["javac" "compile" "jar-copier"]
  :main vip.data-processor)
