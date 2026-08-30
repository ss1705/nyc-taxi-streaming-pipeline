import json
import time
import requests
from kafka import KafkaProducer

#Kafka setup
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

#NYC Taxi data
def fetch_taxi_data(limit=500, offset=0):
    url = "https://data.cityofnewyork.us/resource/gkne-dk5s.json"
    params = {"$limit": limit, "$offset": offset}#, "$order": "pickup_datetime DESC"}
    response = requests.get(url, params=params)
    return response.json()

#Send as Kafka msg
def stream_to_kafka(trips):
    for trip in trips:
        producer.send('taxi-trips', value=trip)
        print(f"Sent trip: {trip.get('pickup_datetime')} | "
              f"fare: ${trip.get('fare_amount')} | "
              f"passengers: {trip.get('passenger_count')}")
        time.sleep(0.1)  
    producer.flush()
    print(f"\nDone. {len(trips)} trips sent to Kafka topic 'taxi-trips'.")

if __name__ == "__main__":
    for offset in [0, 500, 1000, 1500, 2000, 2500]:
        print(f"\nFetching batch at offset {offset}...")
        trips = fetch_taxi_data(limit=500, offset=offset)
        print(f"Fetched {len(trips)} trips. Streaming to Kafka...\n")
        stream_to_kafka(trips)