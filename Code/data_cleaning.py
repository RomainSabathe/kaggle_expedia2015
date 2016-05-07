import pandas as pd

def generic_clean_date_time(dataframe, column_name, suffix, include_hours=False):
    """
    Clean a column called "column_name" from dataframe which must be of type
    pandas.tslib.Timestamp. New columns are added with a "suffix" for disam-
    buiation.
    dataframe: the dataframe we want to modify.
    column_name: the column on which is cleaning is to be processed.
    suffix: used for creating new columns. For instance: "year_suffix"
    include_hours: if hour/minute columns should be added to the dataframe.

    returns: the modified dataframe.
    """

    YEAR_REF = 2013 # used to normalize the years
    dataframe['year_%s' % suffix] = dataframe[column_name].dt.year - YEAR_REF
    dataframe['month_%s' % suffix] = dataframe[column_name].dt.month
    dataframe['day_%s' % suffix] = dataframe[column_name].dt.day

    if include_hours:
        dataframe['hour_%s' % suffix] = dataframe[column_name].dt.hour
        dataframe['minute_%s' % suffix] = dataframe[column_name].dt.minute

    dataframe = dataframe.drop(column_name, 1)
    return dataframe


def clean_date_time(dataframe):
    """
    Clean the "date_time" column.
    date_time <=> moment where the action was performed.
    """
    #TODO: for now, the time is ignored. But we can use it with "morning",
    #      "noon", etc...

    return generic_clean_date_time(dataframe, 'date_time', 'action',
                                   include_hours=True)


def clean_srch_ci(dataframe):
    """
    Clean the "srch_ci" column.
    srch_ci <=> checkin date
    """
    return generic_clean_date_time(dataframe, 'srch_ci', 'checkin')


def clean_srch_co(dataframe):
    """
    Clean the "srch_co" column.
    srch_co <=> checkout date
    """
    return generic_clean_date_time(dataframe, 'srch_co', 'checkout')


def clean_dtypes(dataframe):
    """
    Change the dtypes of all columns to better reflect the nature of the data.
    """
    as_category = ['site_name',
                   'posa_continent',
                   'user_location_country',
                   'user_location_region',
                   'user_location_city',
                   'user_id', 'srch_children_cnt',
                   'is_mobile', 'is_package', 'channel', 'srch_adults_cnt',
                   'srch_rm_cnt', 'srch_destination_id', 'is_booking',
                   'srch_destination_type_id', 'hotel_continent',
                   'hotel_country', 'hotel_market', 'hotel_cluster', 'cnt',]

    as_int64 = ['year_action', 'month_action', 'day_action', 'hour_action',
                'minute_action',
                'year_checkin', 'month_checkin', 'day_checkin',
                'year_checkout', 'month_checkout', 'day_checkout',]

    for column in as_category:
        dataframe[column] = dataframe[column].astype('category', ordered=False)

    for column in as_int64:
        #TODO: check that 'fillna' is legitimate here.
        dataframe[column] = dataframe[column].fillna(0)
        dataframe[column] = dataframe[column].astype('int64')

    return dataframe

def complete_cleaning(dataframe):
    dataframe = clean_date_time(dataframe)
    dataframe = clean_srch_ci(dataframe)
    dataframe = clean_srch_co(dataframe)

    dataframe = clean_dtypes(dataframe)

    return dataframe
