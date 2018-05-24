This is final project for master course msan697, taught by Diane Woodbridge, phd. 

We worked on data from Kaggle research [SF Bay Area Bike Share](https://www.kaggle.com/benhamner/sf-bay-area-bike-share). 

To make use of machine learning techniques and distributed computing principals we learned in the course, a data pipeline "S3 - EC2 (mongo) - EMR cluster" is built.

The specific goal we achieved in this project is we successfully predicted number of overal daily trips based on information such as weather and bike station information, with a high accuracy. We archieved this by cleaning and imputing data, feature engineering, and modeling building. The algorithms we explored included Linear Regression, Random Forest and Gradient Boosting. The complete code for modeling could be found [here](https://github.com/Hatchin/msan697project/blob/master/completeCode.py). 

The tool we use is AWS instances, mongoDB and pyspark, including SparkSQL and Spark Mlib.


