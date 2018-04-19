(defproject vip.data-processor "0.1.0-SNAPSHOT"
  :description "Voting Information Project Data Processor"
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/data.csv "0.1.3"]
                 [org.clojure/data.xml "0.0.8"]
                 [org.clojure/tools.logging "0.4.0"]
                 [ch.qos.logback/logback-classic "1.2.3"]
                 [com.novemberain/langohr "3.5.1"]
                 [joda-time "2.9.3"]
                 [clj-time "0.12.2"]
                 [clj-aws-s3 "0.3.10" :exclusions [joda-time]]
                 [com.climate/clj-newrelic "0.2.1"]
                 [democracyworks/squishy "3.0.1"
                    :exclusions [joda-time
                                 org.slf4j/slf4j-simple
                                 org.slf4j/slf4j-api]]
                 [org.clojure/core.async "0.2.391"]
                 [democracyworks/utility-fns "0.2.0"]
                 [net.lingala.zip4j/zip4j "1.3.2"]
                 [turbovote.resource-config "0.2.0"]
                 [joplin.jdbc "0.3.10"
                  :exclusions [ragtime/ragtime.jdbc]]
                 [ragtime/ragtime.jdbc "0.6.4"]
                 [korma "0.4.3"]
                 [org.clojure/java.jdbc "0.6.1"]
                 [org.postgresql/postgresql "42.0.0" :exclusions [org.slf4j/slf4j-simple
                                                                  org.slf4j/slf4j-api]]
                 [org.xerial/sqlite-jdbc "3.8.11.2"]
                 [commons-lang/commons-lang "2.6"]
                 [xerces/xercesImpl "2.11.0"]
                 [com.fasterxml.woodstox/woodstox-core "5.1.0"]
                 [me.raynes/fs "1.4.6"]]
  :plugins [[com.pupeno/jar-copier "0.4.0"]]
  :profiles {:test {:resource-paths ["test-resources"]
                    :dependencies [[com.github.kyleburton/clj-xpath "1.4.5"]]}
             :dev {:source-paths ["dev-src"]
                   :resource-paths ["dev-resources"]
                   :main dev.core}}
  :test-selectors {:default (complement :postgres)
                   :postgres :postgres
                   :all (constantly true)}
  :repl-options {:init (set! *print-length* 50)}
  :java-agents [[com.newrelic.agent.java/newrelic-agent "3.25.0"]]
  :jar-copier {:java-agents true
               :destination "resources/jars"}
  :prep-tasks ["javac" "compile" "jar-copier"]
  :main vip.data-processor)
