export CLASSPATH=target/classes:`cat classpath.txt`
d=`echo $1 | sed 's/\(....\)\(..\)\(..\)/\1-\2-\3/'`
echo Running PARQUET for $d
java -cp $CLASSPATH  -Xmx9G org.gcd.etl.Main config$1.yml $d gcd-parquet PARQUET
