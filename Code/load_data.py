import pandas as pd
from data_cleaning import complete_cleaning

def import_raw_data(train_or_test='train', source='csv'):
    """
    Returns a dataframe that has not been cleaned.
    train_or_test: can take the values 'train' or 'test'.
    source: can take the values 'csv' or 'HDFS'.
    """
    #TODO: implement the HDFS loading
    raw_data_folder = '../Raw data/'
    if train_or_test == 'train':
        filename = raw_data_folder + 'short_train.csv'
    elif train_or_test == 'test':
        filename = raw_data_folder + 'test.csv'
    else:
        raise Exception('Unknown dataset: %s' % train_or_test)

    if source == 'csv':
        data = pd.read_csv(filename,
                           delimiter=',',
                           header=0,
                           verbose=True,
                           parse_dates=[0, 11, 12],
                           infer_datetime_format=True,
                           warn_bad_lines=True,
                           engine='c')

    return data


def import_cleaned_data(train_or_test='train'):
    """
    Returns a dataframe that has been cleaned.
    train_or_test: can take the values 'train' or 'test'.
    """
    #TODO: implement the HDFS loading
    data = import_raw_data(train_or_test)
    data = complete_cleaning(data)

    return data
