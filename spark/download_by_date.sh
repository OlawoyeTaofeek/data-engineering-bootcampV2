set -e

YEAR=$1
MONTH=$2

URL_PREFIX="https://raw.githubusercontent.com/databricks/Spark-The-Definitive-Guide/refs/heads/master/data/retail-data/by-day"

LOCAL_PREFIX="data/definitive_guide_spark/by_day"
mkdir -p ${LOCAL_PREFIX}

for DAY in {1..31}; do
   FDAY=$(printf "%02d" ${DAY})
    
   LOCAL_FILE="${YEAR}-${MONTH}-${FDAY}.csv"
   LOCAL_PATH="${LOCAL_PREFIX}/${LOCAL_FILE}"
   
   URL="${URL_PREFIX}/${LOCAL_FILE}"

   echo "downloading ${URL} to ${LOCAL_PATH}"

   if wget -q --spider "${URL}"; then
       wget "${URL}" -O "${LOCAL_PATH}"
       echo "download saved to ${LOCAL_PATH}"
   else
       echo "⚠️ File not found for ${LOCAL_FILE}, skipping..."
   fi
done