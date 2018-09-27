if [ "$#" -ne 5 ]; then
   echo "Usage:   ./run_oncloud.sh project-name  bucket-name  mainclass-basename kafka-server topic"
   exit
fi

PROJECT=$1
BUCKET=$2
MAIN=$3
SERVER=$4
TOPIC=$5

echo "project=$PROJECT  bucket=$BUCKET  main=$MAIN server=$SERVER topic=$TOPIC"

mvn -X compile -e exec:java \
 -Dexec.mainClass=$MAIN \
      -Dexec.args="--project=$PROJECT \
      --stagingLocation=gs://$BUCKET/staging/ \
      --tempLocation=gs://$BUCKET/staging/ \
      --output=$PROJECT:demos.streamdemo \
      --bootstrap=$SERVER \
      --inputTopic=$TOPIC \
      --runner=DataflowRunner"