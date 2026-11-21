#!/bin/sh
# Run all module tests. With Hadoop >= 3.4.3 the JDK matrix collapsed to one:
# any JDK >= 21 works everywhere (Lucene 10 needs 21+; Hadoop 3.4.3 no longer
# calls Subject.getSubject()).
set -e
cd "$(dirname "$0")"
mvn -q package -DskipTests "$@"
mvn test "$@"
