from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('preprocess').getOrCreate()

df = spark.read.load('/raw-data')

# for info about data see https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
transform = lambda data: {
    # > vendor
    'vendor.cmt': data['VendorID'] == 1,
    'vendor.vf': data['VendorID'] == 2,
    # end vendor <
    
    'passenger_count': data['passenger_count'],
    'trip_distance': data['trip_distance'],
    
    # > payment rate
    'rate.standard_rate': data['RatecodeID'] == 1,
    'rate.jfk': data['RatecodeID'] == 2,
    'rate.newark': data['RatecodeID'] == 3,
    'rate.now': data['RatecodeID'] == 4,
    'rate.negotiated': data['RatecodeID'] == 5,
    'rate.group': data['RatecodeID'] == 6,
    # end payment rate <
    
    'store_and_fwd_flag': data['store_and_fwd_flag'] == 'Y',
    'pu_location': data['PULocationID'],
    'do_location': data['DOLocationID'],
    
    # > patyment type
    'payment_type.credit_card': data['payment_type'] == 1,
    'payment_type.cash': data['payment_type'] == 2,
    'payment_type.no_charge': data['payment_type'] == 3,
    'payment_type.dispute': data['payment_type'] == 4,
    'payment_type.unknown': data['payment_type'] == 5,
    # end payment type <
    
    'fare_amount': data['fare_amount'],
    'extra': data['extra'],
    'mta_tax': data['mta_tax'],
    'tip_amount': data['tip_amount'],
    'tolls_amount': data['tolls_amount'],
    'improvement_surcharge': data['improvement_surcharge'],
    'total_amount': data['total_amount'],
    'congestion_surcharge': data['congestion_surcharge'],
    'airport_fee': data['Airport_fee'],
    
    # > infered features
    'month': data['tpep_pickup_datetime'].month,
    'day': data['tpep_pickup_datetime'].day,
    'hour': data['tpep_pickup_datetime'].hour,
    'duration': (data['tpep_dropoff_datetime'] - data['tpep_pickup_datetime']).total_seconds(),
    # end infered features <
}

transformed_df = df.rdd.map(transform).toDF()
transformed_df.write.save('/transformed-data', format='parquet')
