"""

This script contains bauplan models that form a complete machine learning pipeline:

clean_taxi_trips:
Performs an S3 scan on the Iceberg table taxi_fhvhv, retrieving a sizable chunk of data. It uses Pandas to clean specific columns for further processing.

training_dataset:
Prepares a training dataset using Pandas and Scikit-learn by selecting features, defining target variables, and normalizing the data distribution.

train_regression_model:
Splits the dataset into train, validation, and test sets, then trains a Linear Regression model. The model is saved in a key-value store, and the test set is returned for use in the next step.

tip_predictions:
Retrieves the trained regression model from the key-value store and uses it to generate predictions on the test set, producing a table of results.

While it is not mandatory to group all models in a single models.py file, we recommend doing so to keep the pipeline code organized and maintainable.

"""

import bauplan


@bauplan.model()
# for this function we specify one dependency, Pandas 2.2.0
@bauplan.python('3.11', pip={'pandas': '2.2.0'})
def clean_taxi_trips(
        data=bauplan.Model(
            'taxi_fhvhv',
            # this function performs an S3 scan directly in Python, so we can specify the columns and the filter pushdown
            # by pushing the filters down to S3 we make the system considerably more performant
            columns=[
                'pickup_datetime',
                'dropoff_datetime',
                'PULocationID',
                'DOLocationID',
                'trip_miles',
                'trip_time',
                'base_passenger_fare',
                'tolls',
                'sales_tax',
                'tips'],
            filter="pickup_datetime >= '2023-01-01T00:00:00-05:00' AND pickup_datetime < '2023-03-31T00:00:00-05:00'"
        )
):
    import math
    import pandas as pd

    # debugging lines to check print the version of Python interpreter and the size of the table
    size_in_gb = data.nbytes / math.pow(1024, 3)
    print(f"This table is {size_in_gb} GB and has {data.num_rows} rows")
    # input data is always an Arrow table, so if you wish to use pandas, you need an explicit conversion
    df = data.to_pandas()
    # exclude rows based on multiple conditions
    df = df[(df['trip_miles'] > 1.0) & (df['tips'] > 0.0) & (df['base_passenger_fare'] > 1.0)]

    # output the data as a Pandas dataframe
    return df


@bauplan.model()
# for this function we specify two dependencies, Pandas 2.2.0 and Scikit-Learn 1.3.2
@bauplan.python('3.10', pip={'pandas': '1.5.3', 'scikit-learn': '1.3.2'})
def training_dataset(
        data=bauplan.Model(
            'clean_taxi_trips',
        )
):
    import pandas as pd
    import numpy as np
    from sklearn.preprocessing import StandardScaler

    # convert data from Arrow to Pandas
    df = data.to_pandas()

    # drop all the rows with NaN values
    df = df.dropna()
    # add a new column with log transformed trip_miles to deal with skewed distributions
    df['log_trip_miles'] = np.log10(df['trip_miles'])
    # define training and target features
    features = df[['log_trip_miles', 'base_passenger_fare', 'trip_time']]
    target = df['tips']
    pickup_dates = df['pickup_datetime']

    # scale the features to ensure that they have similar scales
    # compute the mean and standard deviation for each feature in the training set
    # then use the computed mean and standard deviation to scale the features.
    scaler = StandardScaler()
    scaled_features = scaler.fit_transform(features)
    scaled_df = pd.DataFrame(scaled_features, columns=features.columns)
    scaled_df['tips'] = target.values # Add the target column back to the DataFrame
    scaled_df['pickup_datetime'] = pickup_dates.values  # Add the date column back to the DataFrame

    # print the size of the training dataset
    print(f"The training dataset has {len(scaled_df)} rows")

    # The result is a new array where each feature will have a mean of 0 and a standard deviation of 1
    return scaled_df


@bauplan.model()
# for this function we specify two dependencies, Pandas 2.2.0 and Scikit-Learn 1.3.2
@bauplan.python('3.11', pip={'pandas': '2.2.0', 'scikit-learn': '1.3.2'})
def train_regression_model(
        data=bauplan.Model(
            'training_dataset',
        )
):
    from sklearn.model_selection import train_test_split
    from sklearn.linear_model import LinearRegression

    # convert arrow input into a Pandas DataFrame
    df = data.to_pandas()

    # Define the training and validation set sizes
    training_threshold = 0.8
    validation_threshold = 0.1  # This will implicitly define the test size as the remaining percentage
    # Split the dataset into training and remaining sets first
    train_set, remaining_set = train_test_split(df, train_size=training_threshold, random_state=42)
    # Split the remaining set into validation and test sets
    validation_threshold_adjusted = validation_threshold / (1 - training_threshold)
    validation_set, test_set = train_test_split(remaining_set, test_size=validation_threshold_adjusted, random_state=42)
    # print(f"The training dataset has {len(train_set)} rows")
    print(f"The validation set has {len(validation_set)} rows")
    print(f"The test set has {len(test_set)} rows (remaining)")

    # prepare the feature matrix (X) and target vector (y) for training
    X_train = train_set[['log_trip_miles', 'base_passenger_fare', 'trip_time']]
    y_train = train_set['tips']
    # Train the linear regression model
    reg = LinearRegression().fit(X_train, y_train)

    # persist the model in a key, value store so we can use it later in the DAG
    from bauplan.store import save_obj
    save_obj("regression", reg)

    # Prepare the feature matrix (X) and target vector (y) for validation
    X_test = validation_set[['log_trip_miles', 'base_passenger_fare', 'trip_time']]
    y_test = validation_set['tips']

    # Make predictions on the validation set
    y_hat = reg.predict(X_test)
    # Print the model's mean accuracy (R^2 score)
    print("Mean accuracy: {}".format(reg.score(X_test, y_test)))

    # Prepare the output table with predictions
    validation_df = validation_set[['log_trip_miles', 'base_passenger_fare', 'trip_time', 'tips']]
    validation_df['predictions'] = y_hat

    # Display the validation set with predictions
    print(validation_df.head())

    return test_set


@bauplan.model(materialization_strategy='REPLACE')
# for this function we specify two dependencies, Pandas 2.2.0 and Scikit-Learn 1.3.2
@bauplan.python('3.11', pip={'scikit-learn': '1.3.2', 'pandas': '2.1.0'})
def tip_predictions(
        data=bauplan.Model(
            'train_regression_model',
        )

):

    # retrieve the model trained in the previous step of the DAG from the key, value store
    from bauplan.store import load_obj
    reg = load_obj("regression")
    print(type(reg))

    # convert the test set from an Arrow table to a Pandas DataFrame
    test_set = data.to_pandas()

    # Prepare the feature matrix (X) and target vector (y) for test
    X_test = test_set[['log_trip_miles', 'base_passenger_fare', 'trip_time']]
    y_test = test_set['tips']

    # Make predictions on the test set
    y_hat = reg.predict(X_test)
    # Print the model's mean accuracy (R^2 score)
    print("Mean accuracy: {}".format(reg.score(X_test, y_test)))

    # Prepare the finale output table with the predictions
    prediction_df = test_set[['log_trip_miles', 'base_passenger_fare', 'trip_time', 'tips']]
    prediction_df['predictions'] = y_hat

    # return the prediction dataset
    return prediction_df
