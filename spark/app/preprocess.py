from pyspark.sql import SparkSession
import uuid

spark = SparkSession.builder.appName('Preprocess').getOrCreate()

df = spark.read.load('/raw-data')
df = df.na.drop()

filtered_data = df.filter(
    (df.trip_distance > 0) &
    (df.passenger_count > 0) &
    (df.tpep_dropoff_datetime != df.tpep_pickup_datetime)
)

divide_possible_zero = lambda a, b: (a / b) if b > 0 else 0

# for info about data see https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
transform = lambda data: {
    'id': str(uuid.uuid4()),
    
    # > vendor
    'vendor__cmt': int(data['VendorID'] == 1),
    'vendor__vf': int(data['VendorID'] == 2),    
    # end vendor <
    
    'passenger_count': data['passenger_count'],
    'trip_distance': data['trip_distance'],
    
    # > payment rate
    'rate__standard_rate': int(data['RatecodeID'] == 1),
    'rate__jfk': int(data['RatecodeID'] == 2),
    'rate__newark': int(data['RatecodeID'] == 3),
    'rate__now': int(data['RatecodeID'] == 4),
    'rate__negotiated': int(data['RatecodeID'] == 5),
    'rate__group': int(data['RatecodeID'] == 6),
    # end payment rate <
    
    'store_and_fwd_flag': int(data['store_and_fwd_flag'] == 'Y'),
    'pu_location': data['PULocationID'],
    'do_location': data['DOLocationID'],
    
    # > patyment type
    'payment_type__credit_card': int(data['payment_type'] == 1),
    'payment_type__cash': int(data['payment_type'] == 2),
    'payment_type__no_charge': int(data['payment_type'] == 3),
    'payment_type__dispute': int(data['payment_type'] == 4),
    'payment_type__unknown': int(data['payment_type'] == 5),
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
    'fare_per_passenger': divide_possible_zero(data['total_amount'], data['passenger_count']),
    'fare_per_distance': divide_possible_zero(data['total_amount'], data['trip_distance']),
    'fare_per_duration': divide_possible_zero(data['total_amount'], (data['tpep_dropoff_datetime'] - data['tpep_pickup_datetime']).total_seconds()),
    # end infered features <
}

transformed_df = filtered_data.rdd.map(transform).toDF()
transformed_df = transformed_df.na.drop()
transformed_df.write.mode('overwrite').save('/transformed-data', format='parquet')
