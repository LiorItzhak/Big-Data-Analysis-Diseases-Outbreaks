# v 0.0.1
import pandas as pd
from flask import jsonify
from pandas.api.types import is_numeric_dtype, is_bool_dtype
from datetime import timedelta
from pytrends.request import TrendReq
import numpy as np
import time
from datetime import datetime
from threading import Lock
import requests
from dateutil.relativedelta import relativedelta


# assert that only columns with data are numeric!
# works only on date with type datetime or str with format yyyy-MM-dd
def normalize_by_correlation(old_dataframe, new_dataframe, remove_correlation=True, q_noise_filter=None):
    """normalize the new data frame through the correlation between the old and the new data frames.
       return the normalized new data frame"""
    if old_dataframe is None or old_dataframe.shape[0] == 0:
        return new_dataframe, None, None

    values_cols = [c for c in new_dataframe.columns.values if
                   is_numeric_dtype(new_dataframe[c]) and not is_bool_dtype(new_dataframe[c])]
    new_dataframe_dates = new_dataframe['date']
    old_dataframe_dates = old_dataframe['date']

    assert not new_dataframe_dates.duplicated().any(), f'new_dataframe contains duplicate dates:\n' \
                                                       f'{new_dataframe_dates[new_dataframe_dates.duplicated()]}'

    assert not old_dataframe_dates.duplicated().any(), f'old_dataframe contains duplicate dates:\n' \
                                                       f'{old_dataframe_dates[old_dataframe_dates.duplicated()]}'

    corr_dates = pd.merge(new_dataframe_dates, old_dataframe_dates, how='inner')

    assert corr_dates.shape[0] > 0, "dataframes must have correlation"
    corr_from_date, corr_to_date = corr_dates.values[0][0], corr_dates.values[-1][0]
    delta = corr_to_date - corr_from_date
    print(f'correlation between {corr_from_date} to {corr_to_date}  {int(delta / np.timedelta64(1, "D"))} days')

    # correlation between new_min_date to old_max_date
    # calculate Q value - the mean of the divides between the values of the correlation
    old_corr_df = old_dataframe[old_dataframe['date'].isin(corr_dates['date'])]
    new_corr_df = new_dataframe[new_dataframe['date'].isin(corr_dates['date'])]
    assert old_corr_df.size == new_corr_df.size, f'Data frames correlations must be with the same size' \
                                                 f' {old_corr_df.size}|{new_corr_df.size} || ' \
                                                 f'old_corr_df={old_corr_df.columns},{old_corr_df.shape} ' \
                                                 f'new_corr_df={new_corr_df.columns},{new_corr_df.shape} ' \
                                                 f'old_dataframe={old_dataframe.columns},{old_dataframe.shape} ' \
                                                 f'new_dataframe={new_dataframe.columns},{new_dataframe.shape} '

    corr_div = old_corr_df[values_cols].reset_index(drop=True) / new_corr_df[values_cols].reset_index(drop=True)
    corr_size = corr_div.size
    # temp[(old_correlation_df_val == 0) & (new_correlation_df_val == 0)] = 1  # ignore 0 (set as 1)
    corr_div = corr_div[np.isfinite(corr_div).all(axis=1)]
    corr_div = corr_div.replace([np.inf, -np.inf, 0], np.nan).dropna()
    valid_corr_size = corr_div.size

    if q_noise_filter is not None:  # remove noise
        corr_div_mean = corr_div.mean().mean()
        corr_div_std = corr_div.std()[0]
        corr_div = corr_div[(corr_div < q_noise_filter * corr_div_std + corr_div_mean) &
                            (corr_div > -q_noise_filter * corr_div_std + corr_div_mean)]
    q_value = corr_div.mean().mean()
    print(f'correlation size:{corr_size},valid:{valid_corr_size}, used:{corr_div.size} Q value = {q_value}')

    if remove_correlation:  # remove the correlation dates
        new_dataframe = new_dataframe[~new_dataframe['date'].isin(corr_dates['date'])]

    # normalize the new dataframe by multiply it by the Q value
    normalized_new_df = new_dataframe.copy()
    normalized_new_df[values_cols] = new_dataframe[values_cols] * q_value
    return normalized_new_df, q_value, corr_div.size


def get_from_pytrends(from_date, to_date, kw_list, geo, time_zone=360):
    pytrends = TrendReq(timeout=(5, 60 * 60), tz=time_zone)
    assert from_date < to_date, f'from_date must be before to_date : from_date={from_date} -> to_date={to_date}'
    # pytrends gets days interval only if the query ask for less then 200 days
    delta = to_date - from_date
    assert delta.days <= 250, f'delta between from_date to to_date must be lower then 250 (got {delta.days})'

    print(f'pytrends timeframe = {from_date} to {to_date} , delta = {(to_date - from_date).days}')
    time_frame = f'{from_date.strftime("%Y-%m-%d")} {to_date.strftime("%Y-%m-%d")}'
    pytrends.build_payload(kw_list, timeframe=time_frame, geo=geo)

    df = pytrends.interest_over_time()
    if df.empty:
        print(f'Warning ,pytrends collected df is empty {time_frame}')
        return df
    df.reset_index(inplace=True)
    if (['date', 'isPartial'] + kw_list) not in df.columns.values:
        print(f'Warning received pytrends df with invalid columns :{df.columns}')

    df['date'] = pd.to_datetime(df['date'].astype('datetime64[ns]'))

    if df['isPartial'].dtype == 'bool':  # BUG(pytrends) - some dataframe are str typed
        df = df[(df['isPartial'] == False)]
    else:
        df = df[(df['isPartial'] == False) | (df['isPartial'] == 'False')]

    df.drop(columns=[c for c in df.columns.values if c not in kw_list + ['date']], inplace=True)
    return df


def get_interest_over_time(from_date=None, to_date=datetime.now(), kw_list=None, geo='', time_zone=360,
                           correlation_size=30, progress_per_itr=20, min_correlation_pct=0.30,
                           drop_history_if_invalid=False, q_noise_filter=1.25, sleep=0, prev_df=None):
    """ @:param drop_history_if_invalid determine if you want to take only the valid data or not
        if set to false the returned dataframe will have all te collected dates but the invalid data will be zero
        @:param when data may contain noise you can use the min_correlation_pct to mark the minimum acceptable
        correlation, its a percentage value (0.0 to 1.0), if the used correlation is less then the require correlation
        size the collected data is marked as invalid (zeros)
        """
    assert prev_df is not None or (kw_list is not None and from_date is not None), \
        'choose between prv_dfs to kwlist and from_date'

    kw_geo_zone_txt = f'kwlist={kw_list},geo:{geo},t_z:{time_zone}'
    id_log_txt = f'from {from_date} to {to_date},{kw_geo_zone_txt}'

    print(f'fetch pytrends kwlist={kw_list}, {from_date} to {to_date}, geo:{geo}, time zone:{time_zone}')

    # if None its will create a new dataframe, else its will append to the existing dataframe
    empty_result_df = pd.DataFrame(columns=['date'] + kw_list, dtype='float64')
    empty_result_df['date'] = pd.to_datetime(empty_result_df['date'])
    if prev_df is not None and not prev_df.empty:
        prev_df = prev_df.reset_index()
        assert 'date' in prev_df.columns.values, f'prev_df must contain "date" column ,prev_df:{prev_df.columns}'
        kw_list = [c for c in prev_df.columns.values if is_numeric_dtype(prev_df[c]) and not is_bool_dtype(prev_df[c])]
        from_date = prev_df['date'].max() - timedelta(days=correlation_size)
        if from_date < prev_df['date'].min():
            from_date = prev_df['date'].min()
        print(f'append from {prev_df["date"].max()} to {to_date}')
        result_df = prev_df
    else:
        result_df = empty_result_df.copy()

    min_correlation_size = int(min_correlation_pct * correlation_size)
    queries_distance = progress_per_itr + correlation_size
    to_date_tmp = from_date + timedelta(days=queries_distance)
    from_date_tmp = from_date
    empty_retry = 0

    while to_date_tmp <= to_date:
        id_tmp_log_txt = f'from {from_date_tmp} to {to_date_tmp},{kw_geo_zone_txt}'
        new_df = get_from_pytrends(from_date_tmp, to_date_tmp, kw_list, geo, time_zone)
        q_factor = 1  # normal Q factor
        # handle empty dataframe
        if new_df.empty:
            if from_date_tmp == from_date:
                #  there is no data to collect
                print(f'Warning ,collected df is empty {id_tmp_log_txt}- add 1 year')
                from_date_tmp = from_date_tmp + relativedelta(years=1)
                from_date = from_date_tmp
                if from_date_tmp > to_date:
                    print(f'Warning ,there is no data to collect {id_log_txt}')
                    result_df = empty_result_df  # will be empty df
                    break  # end
                to_date_tmp = from_date_tmp + timedelta(days=queries_distance)
                if to_date_tmp > to_date:
                    to_date_tmp = to_date
                continue  # try again
            elif empty_retry < 0:  # ignore retry
                # sometimes there is a BUG in pytrends that fails to collect specific date's range - try different range
                print(f'Warning ,collected df is empty {id_tmp_log_txt}, retry {empty_retry}')
                empty_retry = empty_retry + 1
                from_date_tmp = from_date_tmp + timedelta(days=1)
                continue  # try again
            else:
                # padding with zeros
                print(f'Warning ,collected df is empty {id_tmp_log_txt}, failed {empty_retry} times - return None')
                new_df = empty_result_df.copy()
                dates = [from_date_tmp + timedelta(days=d) for d in range(0, queries_distance)]
                values = [[date, 0.0] for date in dates]
                for i in range(0, queries_distance):
                    new_df.loc[i] = values[i]
                # break

        empty_retry = 0
        assert 'date' in new_df.columns.values, f'new_df must contain "date" column |{new_df.columns},{id_tmp_log_txt}'

        # if previous dataframe available- normalize the new_df with their correlation
        if prev_df is not None and not prev_df.empty and prev_df['date'].max() > new_df['date'].min():
            prev_df = prev_df[prev_df['date'] >= new_df['date'].min()]

            if not (prev_df[kw_list] == 0).all(axis=None):
                new_df, q_factor, correlation_used = normalize_by_correlation(prev_df, new_df,
                                                                              q_noise_filter=q_noise_filter)
                if correlation_used < min_correlation_size:
                    q_factor = None
                    print(f'correlation_used {correlation_used} is less the required {min_correlation_size}')

        assert new_df is not None, 'new_df cant be None'
        # invalid Q factor indicate that there is not enough data to calculate correlation or inconsistent dataframes
        q_factor_invalid = q_factor is None or np.math.isnan(q_factor) or q_factor == 0 or q_factor == np.nan
        # handle invalid correlation
        if q_factor_invalid:
            print(f'the received dataset is invalid (bad Q factor): {q_factor} | {id_tmp_log_txt}')
            if drop_history_if_invalid:
                # drop all history and start collection new data from this point
                print(f'history dropped - until {to_date_tmp}')
                new_df, result_df, prev_df = None, empty_result_df, None
            else:  # mask invalids with zeros
                print(f'mask invalids with zeros {to_date_tmp - timedelta(days=progress_per_itr)} | {id_tmp_log_txt}')
                new_df[kw_list] = 0

        # append the new data to the existing result
        if new_df is not None:
            if not result_df.empty:
                new_df = new_df[new_df['date'] > result_df['date'].max()]  # append only the new dates
            print(f'appending result {id_tmp_log_txt} | size:{new_df.shape[0]}')

            result_df = result_df.append(new_df, ignore_index=True, sort=False)

        # use the result for later df normalizations (by their correlation)
        prev_df = result_df

        # increments from_date_tmp and to_date_tmp to their next time window
        if from_date_tmp + timedelta(days=queries_distance) > to_date:
            progress_per_itr = correlation_size - (to_date - from_date_tmp).days
        from_date_tmp = from_date_tmp + timedelta(days=progress_per_itr)
        to_date_tmp = from_date_tmp + timedelta(days=queries_distance)
        time.sleep(sleep)

    print(f'complete fetching data - {id_log_txt}')
    return result_df.set_index('date')


lock = Lock()
ip = requests.get('https://api.ipify.org').text


def main(request):
    global lock, ip
    print(f'my ip:{ip}')
    if lock.locked():
        return f'instance {ip} is taken!', 429
    lock.acquire()
    try:
        request_json = request.get_json(silent=True)
        kw = request_json['kw']
        region = request_json['region']
        from_date = request_json['fromDate']
        to_date = request_json['toDate']

        # fetching data from Google Trends
        df = get_interest_over_time(prev_df=None,
                                    from_date=datetime.strptime(from_date, '%Y-%m-%d'),
                                    to_date=datetime.strptime(to_date, '%Y-%m-%d'),
                                    kw_list=[kw], geo=region,
                                    correlation_size=60, progress_per_itr=30, min_correlation_pct=0.2,
                                    q_noise_filter=1.2, drop_history_if_invalid=False)
        res = []
        # each record:
        # {“kw”: “Cough”, “date”: “2020-01-01T00:01:00”, “region” : “US” , “value” :12 }
        for date, row in df.iterrows():
            res.append({'datetime': date.isoformat(), "kw": kw, "region": region, "value": row[kw]})
        data_json = jsonify(res)
        # Stream to Kafka
        producer = KafkaProducer(bootstrap_servers=['KAFKA_IP:9092'],
                                 value_serializer=lambda x:
                                 dumps(x).encode('utf-8'))
        producer.send("KAFKA_TOPIC", value=data_json)
        return jsonify(res)
    except Exception as e:
        return f'{type(e)} in {ip}: {e}', 500
    finally:
        lock.release()

