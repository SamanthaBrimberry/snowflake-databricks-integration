# Databricks notebook source
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

# MAGIC %pip install faker

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

query = f'''
CREATE SCHEMA IF NOT EXISTS {catalog}.{schema};
'''
spark.sql(query)

# COMMAND ----------

query = f'''
DROP TABLE {catalog}.{schema}.theme_park_visitors;
'''
spark.sql(query)

# COMMAND ----------

reviews_df = (
  spark
  .read
  .format("text")
  .option("wholetext", "true")
  .load(f"/Volumes/{catalog}/{schema}/reviews/*.txt")
  .withColumn('id', monotonically_increasing_id()))

(reviews_df
 .write
 .format('delta')
 .mode('overwrite')
 .saveAsTable(f'{catalog}.{schema}.theme_park_reviews'))

# COMMAND ----------

from faker import Faker
import pandas as pd
import random
import numpy as np

fake = Faker()

def create_theme_park_dataset(num_records=500):
    # Define categorical options
    high_demand_rides = [
        'Harry Potter and the Escape', 'Jurassic Park River Adventure', 
        'The Incredible Hulk Coaster', 'Transformers: The Ride', 
        'Spider-Man Ride', 'Revenge of the Mummy', 'Hagrid\'s Magical Creatures Motorbike Adventure'
    ]
    group_types = ['Family (2 adults)', 'Family (2+2)', 'Solo', 'Couple', 'Friends (3-4)', 'Large Group (5+)']
    visit_timings = ['Peak (Summer)', 'Peak (Holiday)', 'Off-Peak (Weekday)', 'Off-Peak (Weekend)']
    ticket_types = ['One-Day', 'Multi-Day', 'Annual Pass', 'VIP Experience']
    
    data = []
    for _ in range(num_records):
        # Transactional History
        past_skip_pass = 1 if random.random() < 0.35 else 0  # 35% purchased before
        visit_frequency = abs(random.gauss(1, 15))  # Visits per year
        total_spending = round(random.gauss(800, 300), 2)  # Normal distribution
        total_spending = max(50, min(2000, total_spending))  # Cap values
        
        # Behavioral Indicators
        avg_queue_time = round(random.triangular(10, 120, 40), 1)  # Most common around 40 min
        avg_ride_time = round(random.uniform(2.5, 8.5), 1)
        preferred_ride = random.choice(high_demand_rides)
        
        # Demographic & Contextual Data
        group_type = random.choice(group_types)
        visit_timing = random.choice(visit_timings)
        ticket_type = random.choice(ticket_types)
        age_group = random.choice(['18-24', '25-34', '35-44', '45-54', '55+'])
        
        # Attitudinal Signals
        wait_tolerance = random.choices([1, 2, 3, 4, 5], weights=[0.15, 0.25, 0.3, 0.2, 0.1])[0]
        sentiment_score = round(np.random.normal(loc=0.1, scale=0.6), 2)  # Slightly positive skew
        sentiment_score = max(-1.0, min(1.0, sentiment_score))  # Bound between -1 and 1
        
        # Additional realistic features
        queue_complaints = max(0, int(np.random.poisson(lam=0.7)))  # Poisson distribution for complaint count
        last_visit = fake.date_between(start_date='-1y', end_date='today')
        
        data.append([
            _ + 1,                      # Customer ID
            int(past_skip_pass),               # Past skip-the-line purchase
            float(visit_frequency),            # Visit frequency
            float(total_spending),             # Total spending
            float(avg_queue_time),             # Average queue time
            float(avg_ride_time),              # Average ride time
            preferred_ride,                    # Preferred ride
            group_type,                        # Group type
            visit_timing,                      # Visit timing
            ticket_type,                       # Ticket type
            age_group,                         # Age group
            # int(wait_tolerance),               # Wait tolerance (survey)
            # float(sentiment_score),            # Sentiment score
            # int(queue_complaints),             # Queue complaints count
            last_visit                         # Last visit date
        ])
    
    columns = [
        'CustomerID', 'PastSkipPass', 'VisitFrequency', 'TotalSpendingUSD', 
        'AvgQueueTimeMin', 'AvgRideTimeMin', 'PreferredRide', 'GroupType', 
        'VisitTiming', 'TicketType', 'AgeGroup', 'LastVisitDate'
    ]
    
    return spark.createDataFrame(data, columns)

# Generate dataset
(create_theme_park_dataset(500)
 .write
 .mode('overwrite')
 .format('delta')
 .saveAsTable(f'{catalog}.{schema}.theme_park_visitors')
 )

# COMMAND ----------

from pyspark.sql.functions import *
import faker
import pandas as pd
import random

fake = faker()

def create_theme_park_customer_data(num_records=100):
    park_names = ['Universal Studios Florida', 'Islands of Adventure', 'Volcano Bay']
    ticket_types = ['One-Day', 'Multi-Day', 'Annual Pass']
    data = []
    for _ in range(num_records):
        customer_id = fake.uuid4()
        first_name = fake.first_name()
        last_name = fake.last_name()
        email = fake.email()
        visit_date = fake.date_between(start_date='-2y', end_date='today')
        park_name = random.choice(park_names)
        ticket_type = random.choice(ticket_types)
        age = random.randint(5, 80)
        review_rating = random.randint(1, 5)
        data.append([
            customer_id, first_name, last_name, email, visit_date,
            park_name, ticket_type, age, review_rating
        ])
    columns = [
        'CustomerID', 'FirstName', 'LastName', 'Email', 'VisitDate',
        'ParkName', 'TicketType', 'Age', 'City', 'Country', 'ReviewRating', 'ReviewText'
    ]
    df = pd.DataFrame(data, columns=columns)
    return df

# Generate and display a sample dataset
sample_df = create_theme_park_customer_data(10)
print(sample_df.head())


# COMMAND ----------

### This is just an example
### Verified users can access all reviews on for their business via API

import requests

API_KEY = "YOUR_GOOGLE_PLACES_API_KEY"  # Replace with your actual API key

# Step 1: Find the place_id for Universal Epic Universe
search_url = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json"
params = {
    "input": "Universal Epic Universe, Orlando",
    "inputtype": "textquery",
    "fields": "place_id",
    "key": API_KEY
}
response = requests.get(search_url, params=params)
place_id = response.json()["candidates"][0]["place_id"]

# Step 2: Get place details (including reviews)
details_url = "https://maps.googleapis.com/maps/api/place/details/json"
details_params = {
    "place_id": place_id,
    "fields": "name,formatted_address,rating,review",
    "key": API_KEY
}
details_response = requests.get(details_url, params=details_params)
result = details_response.json()["result"]

# Print the business name and up to 5 reviews
print("Business Name:", result.get("name"))
for review in result.get("reviews", []):
    print("Author:", review.get("author_name"))
    print("Rating:", review.get("rating"))
    print("Text:", review.get("text"))
    print("-----")
