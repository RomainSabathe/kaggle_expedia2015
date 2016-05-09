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
        columns_dates = [0, 11, 12]  # for parsing
    elif train_or_test == 'test':
        filename = raw_data_folder + 'short_test.csv'
        columns_dates = [1, 12, 13]  # for parsing
    else:
        raise Exception('Unknown dataset: %s' % train_or_test)

    if source == 'csv':
        data = pd.read_csv(filename,
                           delimiter=',',
                           header=0,
                           verbose=False,
                           parse_dates=columns_dates,
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


def import_features_and_target(train_or_test='train'):
    """
    Returns an augmented and cleaned dataset (as X) and its corresponding
    target values (y) when asked for the train dataset. None is returned
    otherwise.
    """
    df = import_cleaned_data(train_or_test)
    X = df.iloc[:, df.columns != 'hotel_cluster']
    y = df.iloc[:, 'hotel_cluster'] if train_or_test=='train' else None

    return X,y

#TODO: need to fix the fact that df_test['srch_ci'] is not parsed as a timedate
df_train = import_raw_data('train')
df_test  = import_raw_data('test')
df_train.info()
df_test.info()
import pdb; pdb.set_trace()
