#!/bin/bash

function install_maven {
	group=$1
	artifact=$2

	jarfile=`ls lib/$artifact*.jar`
	pomfile=`ls lib/$artifact*.pom`
	version=`echo $pomfile | sed "s/lib.$artifact-//;s/.pom//;"`

	mvn install:install-file -Dfile=$jarfile -DgroupId=$group -DartifactId=$artifact -Dversion=$version -Dpackaging=jar -DpomFile=$pomfile
}

install_maven com.indeed imhotep-server
install_maven com.indeed imhotep-client
install_maven com.indeed imhotep-archive

