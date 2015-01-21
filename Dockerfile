FROM clojure:lein-2.5.0

RUN mkdir -p /usr/src/data-processor
WORKDIR /usr/src/data-processor

COPY project.clj /usr/src/data-processor/
RUN lein deps

COPY . /usr/src/data-processor

RUN mv "$(lein uberjar | sed -n 's/^Created \(.*standalone\.jar\)/\1/p')" data-processor-standalone.jar

CMD ["java", "-jar", "data-processor-standalone.jar"]
