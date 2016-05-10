import os
import pandas as pd
from data_cleaning import complete_cleaning

def import_raw_data(train_or_test='train', by_chunk=True, source='csv'):
    """
    Returns a dataframe that has not been cleaned.
    train_or_test: can take the values 'train' or 'test'.
    source: can take the values 'csv' or 'HDFS'.
    """
    #TODO: implement the HDFS loading
    raw_data_folder = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                   '../Raw data/')
    if train_or_test == 'train':
        filename = os.path.join(raw_data_folder,  './short_train.csv')
        columns_dates = [0, 11, 12]  # for parsing
    elif train_or_test == 'test':
        filename = os.path.join(raw_data_folder, './short_test.csv')
        columns_dates = [1, 12, 13]  # for parsing
    else:
        raise Exception('Unknown dataset: %s' % train_or_test)

    #TODO: better handling of this error?
    date_parser = lambda x: pd.to_datetime(x, errors='coerce') # to avoid
        # "ValueError: day is out of range" that sometimes occur (srch_ci)

    # Setting up the parameters of the csv reading
    parameters = {'delimiter':',',
                  'header':0,
                  'verbose':False,
                  'parse_dates':columns_dates,
                  'infer_datetime_format':True,
                  'date_parser':date_parser,
                  'warn_bad_lines':True,
                  'engine':'c',}
    if by_chunk:
        parameters['chunksize'] = 10000

    if source == 'csv':
        data = pd.read_csv(filename, **parameters)

    return data


def import_cleaned_data(train_or_test='train'):
    """
    Returns a dataframe that has been cleaned.
    train_or_test: can take the values 'train' or 'test'.
    """
    #TODO: implement the HDFS loading
    data = import_raw_data(train_or_test, by_chunk=False)
    data = complete_cleaning(data)

    return data


def import_features_and_target(train_or_test='train'):
    """
    Returns an augmented and cleaned dataset (as X) and its corresponding
    target values (y) when asked for the train dataset. None is returned
    otherwise.
    """
    df = import_cleaned_data(train_or_test, by_chunk=False)

    return get_features_and_target(df)


def get_features_and_target(df):
    """
    Takes a full dataset as argument and returns the corresponding X (features)
    and y (target) dataframes.
    """
    X = df.iloc[:, df.columns != 'hotel_cluster']

    # Deals with the case where the dataframe is the test one
    y = df['hotel_cluster'] if 'hotel_cluster' in df.columns  else None

    return X, y
