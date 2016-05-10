import pandas as pd
import warnings

def generic_clean_date_time(df, column_name, suffix, include_hours=False):
    """
    Clean a column called "column_name" from df which must be of type
    pandas.tslib.Timestamp. New columns are added with a "suffix" for disam-
    buiation.
    df: the df we want to modify.
    column_name: the column on which is cleaning is to be processed.
    suffix: used for creating new columns. For instance: "year_suffix"
    include_hours: if hour/minute columns should be added to the df.

    returns: the modified df.
    """

    YEAR_REF = 2013 # used to normalize the years
    if column_exists(column_name, df):
        df['year_%s' % suffix] = df[column_name].dt.year - YEAR_REF
        df['month_%s' % suffix] = df[column_name].dt.month
        df['day_%s' % suffix] = df[column_name].dt.day

        if include_hours:
            df['hour_%s' % suffix] = df[column_name].dt.hour
            df['minute_%s' % suffix] = df[column_name].dt.minute

        df = df.drop(column_name, 1)

    return df


def clean_date_time(df):
    """
    Clean the "date_time" column.
    date_time <=> moment where the action was performed.
    """
    #TODO: for now, the time is ignored. But we can use it with "morning",
    #      "noon", etcraw

    return generic_clean_date_time(df, 'date_time', 'action',
                                   include_hours=True)


def clean_srch_ci(df):
    """
    Clean the "srch_ci" column.
    srch_ci <=> checkin date
    """
    return generic_clean_date_time(df, 'srch_ci', 'checkin')


def clean_srch_co(df):
    """
    Clean the "srch_co" column.
    srch_co <=> checkout date
    """
    return generic_clean_date_time(df, 'srch_co', 'checkout')


def clean_dtypes(df):
    """
    Change the dtypes of all columns to better reflect the nature of the data.
    """
    as_category = ['site_name',
                   'posa_continent',
                   'user_location_country',
                   'user_location_region',
                   'user_location_city',
                   'user_id',
                   'channel',
                   'srch_destination_id',
                   'srch_destination_type_id', 'hotel_continent',
                   'hotel_country', 'hotel_market', 'hotel_cluster', 'cnt',]

    as_int64 = ['year_action', 'month_action', 'day_action', 'hour_action',
                'minute_action',
                'year_checkin', 'month_checkin', 'day_checkin',
                'year_checkout', 'month_checkout', 'day_checkout',]

    as_bool = ['is_package', 'is_mobile', 'srch_children_cnt', 'srch_rm_cnt',
               'is_booking', 'srch_adults_cnt',]


    for column in as_category:
        if column_exists(column, df):
            df[column] = df[column].astype('category', ordered=False)

    for column in as_int64:
        if column_exists(column, df):
            #TODO: error here
            df[column] = df[column].astype('int64')

    for column in as_bool:
        if column_exists(column, df):
            df[column] = df[column].astype('bool')

    return df

def complete_cleaning(df):
    df = clean_date_time(df)
    df = clean_srch_ci(df)
    df = clean_srch_co(df)

    df = clean_dtypes(df)

    #TODO: handle more properly missing data
    df = df.fillna(0)

    return df


def column_exists(column, df):
    """
    Is used to enable generalization between training and testing sets which
    do not share exactly the same columns.
    Returns true if 'column' is among df's columns.
    """
    if column in df.columns:
        return True
    else:
        print "WARNING: Column '%s' was ignored because it is missing." % column
        return False
