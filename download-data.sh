BASE_URL="https://d37ci6vzurychx.cloudfront.net/trip-data/"

DATASETS=(
    "yellow_tripdata_"
    # "green_tripdata_"
    # "fhv_tripdata_"
)

YEARS=("2023")
MONTHS=("07" "08" "09" "10" "11" "12")

DOWNLOAD_DIR="hadoop/data/"

mkdir -p $DOWNLOAD_DIR

download_file() {
    local url=$1
    local output_dir=$2
    
    if wget -q --spider "$url"; then
        wget -P "$output_dir" "$url"
    else
        echo "File not found: $url"
    fi
}

for dataset in "${DATASETS[@]}"; do
    for year in "${YEARS[@]}"; do
        for month in "${MONTHS[@]}"; do
            file_url="${BASE_URL}${dataset}${year}-${month}.parquet"
            download_file "$file_url" "$DOWNLOAD_DIR"
        done
    done
done

echo "Download completed."
