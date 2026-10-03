#!/bin/bash -e

PATH=/opt/homebrew/bin:/usr/local/bin:$PATH

D=$1
S=`echo $1 | tr -d '-'`
Y=`echo $1 | sed "s/-.*//"`

if [ -z "$mysqlpassword" ]; then
  echo -n "MySQL gcd password: "
  read -s mysqlpassword
  echo ""
  mysql --defaults-file=<(echo '[client]'; echo 'user=gcd'; echo "password=$mysqlpassword";) -e ""

  echo "Connection verified."
fi

if [ ! -f gcd-dump-$D.zip ] ; then
#  ./monitor-url.sh $DL
#  ./download.sh $D $DL
  # this stores the URL for a mac launchd triggered shortcut to download via browser
  # this will retry until download appears to succeed
#  ./mac-download.sh $D
  if [ ! -f gcd-dump-$D.zip ] ; then
    echo Attempt to download appears to have failed
    exit 1
  fi
fi

rm -f $D.sql
unzip gcd-dump-$D*.zip

mysql --defaults-file=<(echo '[client]'; echo 'user=gcd'; echo "password=$mysqlpassword";) -e "drop database if exists gcd$S; create database gcd$S;" -vvv

echo "Creating database from $D.sql"
mysql --defaults-file=<(echo '[client]'; echo 'user=gcd'; echo "password=$mysqlpassword";) gcd$S < $D.sql

if [[ "$S" < "20160901" ]]; then
  echo "Added stddata missing from dump"
  mysql --defaults-file=<(echo '[client]'; echo 'user=gcd'; echo "password=$mysqlpassword";) gcd$S < stddata.sql
fi

if [ ! -f config$S.yml ] ; then
  sed "s/DATESTAMP/$S/" $Y-template.yml > config$S.yml
fi

echo "Build parquet"
./run-parquet.sh $S
./upload-hdfs.sh $S

#echo "Build flamdex"
#./run-flamdex.sh $S
#./upload-imhotep.sh $S
#rm -rf index/*-1/

mysql --defaults-file=<(echo "[client]"; echo "user=gcd"; echo "password=$mysqlpassword";) -e "drop database gcd$S" -vvv

rm -f $D.sql
aws s3 cp gcd-dump-$D* s3://gcd-archive/
rm -f gcd-dump-$D*
aws s3 sync gcd-parquet/ s3://gcd-archive/gcd-parquet/

#MD=`echo $S | sed "s/^....0\?//"`

#echo "Update snapshots"
#python src/main/python/refresh_query.py 25

#echo "Update Issue Count By Snapshot query (22)"
#python src/main/python/update_queries.py $MD 2 22
#echo "Re-run at https://redash.gcdata.org/queries/22"
#echo "Refresh results for 22 -- please check in browser"
#python src/main/python/refresh_query.py 22

#echo "Update Stories Using New Credit System, By Snapshot query (5)"
#python src/main/python/update_queries.py $MD 6 5
#echo "Re-run at https://redash.gcdata.org/queries/5"
#echo "Refresh results for 5 -- please check in browser"
#python src/main/python/refresh_query.py 5
