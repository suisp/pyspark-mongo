# coding: utf-8
import pandas as pd
import math
import timeit
import multiprocessing

EQUAL_RATE = 2.75
TAX_RATE = .085

current_year = 2018
end_year = current_year - 1
start_year = end_year - 2

sim_cols = ['AGE', 'BUILDING_SQ_FT', 'radius', 'EXT_NUM', 'BSMT_NUM', 'RES_NUM']

EXTERIOR = {
    'Frame': 1,
    'Frame/Masonry': 2,
    'Stucco': 3,
    'Masonry': 4
}

BASEMENT = {
    'Full and Apartment': 8,
    'Full and Formal Rec. Room': 7,
    'Full and Unfinished': 6,
    'Partial and Apartment': 5,
    'Partial and Formal Rec. Room': 4,
    'Partial and Unfinished': 3,
    'Slab and Unfinished': 2,
    'Crawl and Unfinished': 1,
}

RES_TYPE = {
    'One Story': 1,
    '1.5 - 1.9': 2,
    'Two Story': 3,
    'Three Story': 4,
    'Multi-Level': 5,
}


def distance(origin, destination):
    lat1, lon1 = origin
    lat2, lon2 = destination
    radius = 3956  # km

    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) * math.sin(dlat / 2) + math.cos(math.radians(lat1)) * math.cos(
        math.radians(lat2)) * math.sin(dlon / 2) * math.sin(dlon / 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    d = radius * c

    return d


def rank_age(x):
    if x < 0 or x in range(0, 4):
        return 5
    elif x in range(4, 8):
        return 4
    elif x in range(8, 12):
        return 3
    elif x in range(12, 16):
        return 2
    elif x in range(16, 20):
        return 1
    else:
        return 0


def rank_radius(x):
    if x < 0 or 0 <= x <= 0.1:
        return 5
    elif 0.1 < x <= 0.2:
        return 4
    elif 0.2 < x <= 0.3:
        return 3
    elif 0.3 < x <= 0.4:
        return 2
    elif 0.4 < x <= 0.5:
        return 1
    else:
        return 0


def rank_sqft(x):
    if x in range(0, 101):
        return 5
    elif x in range(101, 200):
        return 4
    elif x in range(201, 300):
        return 3
    elif x in range(301, 400):
        return 2
    elif x in range(401, 500):
        return 1
    else:
        return 0


def similarity(index, subject, comps):
    usecols = ['PIN'] + sim_cols
    subj = pd.DataFrame(subject).T

    data = pd.concat([subj, comps[usecols]], sort=False).reset_index(drop=True)
    data = data[usecols].reset_index(drop=True)

    data['AGE_DIFF'] = data.AGE - subject.AGE
    data['AGE_DIFF_RANK'] = data.AGE_DIFF.apply(rank_age)
    data['AGE_DIFF_SCORE'] = data.AGE_DIFF_RANK / data.AGE_DIFF_RANK.values[0]

    data['SQFT_DIFF'] = (data.BUILDING_SQ_FT - subject.BUILDING_SQ_FT).abs()
    data['SQFT_DIFF_RANK'] = data.SQFT_DIFF.apply(rank_sqft)
    data['SQFT_DIFF_SCORE'] = data.SQFT_DIFF_RANK / data.AGE_DIFF_RANK.values[0]

    data['RADIUS_RANK'] = data.radius.apply(rank_radius)
    data['RADIUS_SCORE'] = data.RADIUS_RANK / (data.RADIUS_RANK.max() - data.RADIUS_RANK.min())

    scores = ['AGE_DIFF_SCORE', 'SQFT_DIFF_SCORE', 'RADIUS_SCORE']
    for c in ['EXT_NUM', 'BSMT_NUM', 'RES_NUM']:
        # col = '%s_RANK' % c
        score = '%s_SCORE' % c

        data[score] = 0.0
        if subject[c] > 0:
            data[score] = data[c] / subject[c]
        elif data[c].max() > 0:
            data[score] = data[c] / data[c].max()
        elif (data[c] == 0).all():
            data[score] = 1.0
        data.loc[data[score] > 1, score] = 1.0
        scores.append(score)

    data['SCORE_SUM'] = data[scores].sum(axis=1)

    return data[['PIN', 'SCORE_SUM']]


def _calc_avsaving(subject, saving, non_saving, top=3):
    # positive = comps[comps.comp_savings > 0]
    if len(saving) >= top:
        top_k = saving.sort_values(['comp_savings'], ascending=False).iloc[:top]
        subject['avsaving_med_top%s' % top] = top_k['comp_savings'].median()
        subject['avsaving_avg_top%s' % top] = top_k['comp_savings'].mean()

        top_sim_k = saving.sort_values(['SCORE_SUM', 'comp_savings'], ascending=False).iloc[:top]
        subject['avsaving_med_sim_top%s' % top] = top_sim_k['comp_savings'].median()
        subject['avsaving_avg_sim_top%s' % top] = top_sim_k['comp_savings'].mean()
        subject['avsaving_score_top%s' % top] = top_sim_k['SCORE_SUM'].median()
    if len(non_saving) >= top:
        top_sim_k = non_saving.sort_values(['SCORE_SUM'], ascending=False).iloc[:top]
        subject['avsaving_score_lose_top%s' % top] = top_sim_k['SCORE_SUM'].median()


def _calc_mvsaving(subject, comps, end_year, top=3):
    years = [end_year]
    find = 'NOT FOUND'
    for j in range(3):
        current_comps = comps[comps.SALE_DATE.dt.year.isin(years)]

        check = current_comps.groupby(['saving_flag'], as_index=False).agg({'PIN': 'count'})
        if check[check.saving_flag == 1].PIN.sum() < top and check[check.saving_flag == 0].PIN.sum() >= 3:
            find = 'LOSE'
            # subject['mvsaving_score_top%s' % top] = top_sim_k['SCORE_SUM'].median()
            break
        elif check[check.saving_flag == 1].PIN.sum() >= top:
            saving = current_comps[current_comps.saving_flag == 1]
            non_saving = current_comps[current_comps.saving_flag == 0]

            assert len(saving) >= top, 'for debug'

            top_k = saving.sort_values(['etav_mv', 'SALE_DATE'], ascending=False).iloc[:top]
            subject['mvsaving_med_top%s' % top] = top_k['etav_mv'].median() * TAX_RATE
            subject['mvsaving_avg_top%s' % top] = top_k['etav_mv'].mean() * TAX_RATE

            top_sim_k = saving.sort_values(['SCORE_SUM', 'SALE_DATE'], ascending=False).iloc[:top]
            subject['mvsaving_med_sim_top%s' % top] = top_sim_k['etav_mv'].median() * TAX_RATE
            subject['mvsaving_avg_sim_top%s' % top] = top_sim_k['etav_mv'].mean() * TAX_RATE
            subject['mvsaving_score_top%s' % top] = top_sim_k['SCORE_SUM'].median()

            if len(non_saving) >= top:
                top_sim_k = non_saving.sort_values(['SCORE_SUM'], ascending=False).iloc[:top]
                subject['mvsaving_score_lose_top%s' % top] = top_sim_k['SCORE_SUM'].median()

            find = 'WIN'
            break
        else:
            years.append(end_year - j)
    subject['msaving_find_top%s' % top] = find


def calc_saving(index, subject, df, age_range=15, radius_range=1):
    # filter comp
    comps = df.copy()
    comps['AGE_DIFF'] = (comps.AGE - subject.AGE).abs()
    comps['SQ_FT_DIFF'] = (comps.BUILDING_SQ_FT - subject.BUILDING_SQ_FT).abs()
    comps = comps[
        (comps.PIN != subject.PIN) & (comps.NEIGHBORHOOD == subject.NEIGHBORHOOD) & (comps.TOWN == subject.TOWN) & (
        comps.OVACLS == subject.OVACLS) & (comps.AGE_DIFF <= age_range)].reset_index(drop=True)

    # subject['avsaving'] = 0.0
    # subject['mvsaving'] = 0.0

    if len(comps) <= 2:
        return subject

    comps['radius'] = comps.apply(lambda x: distance((x.GPS_LON, x.GPS_LAT), (subject.GPS_LON, subject.GPS_LAT)),
                                  axis=1)
    comps = comps[comps.radius < radius_range].reset_index(drop=True)

    if len(comps) <= 2:
        return subject
    else:
        comps['etav_av'] = 0
        comps['etav_mv'] = 0

        # calc similarities
        comps = comps.merge(similarity(index, subject, comps), on='PIN', how='left')

        # calc 1
        comps['ebav'] = (subject.price_bld - comps.price_bld) * subject.BUILDING_SQ_FT
        comps['elav'] = (subject.price_lot - comps.price_lot) * subject.LAND_SQ_FT
        comps['etav_av'] = 0
        comps.loc[comps.ebav + comps.elav > 0, 'etav_av'] = comps.ebav + comps.elav
        comps['comp_savings'] = comps.etav_av * EQUAL_RATE * TAX_RATE

        subject['all_score'] = comps.SCORE_SUM.median()
        saving = comps[comps.comp_savings > 0]
        non_saving = comps[comps.comp_savings == 0]
        _calc_avsaving(subject, saving, non_saving, top=3)
        _calc_avsaving(subject, saving, non_saving, top=4)
        _calc_avsaving(subject, saving, non_saving, top=5)

        # calc 2
        if subject.OVACLS != 241:
            comps['etav_mv'] = 0
            comps.loc[(comps.SALE_DATE >= '01-01-{}'.format(start_year)) & (comps.SALE_DATE <= '12-31-{}'.format(
                end_year)), 'etav_mv'] = subject.CURRENT_BUILDING - subject.BUILDING_SQ_FT * (
            comps.SALE_AMOUNT / 10 - comps.CURRENT_LAND) / comps.BUILDING_SQ_FT
            comps.loc[comps.SALE_AMOUNT / 10 - comps.CURRENT_LAND < 0, 'etav_mv'] = 0.0
            comps.loc[comps.etav_mv < 0, 'etav_mv'] = 0.0
            comps['saving_flag'] = 0
            comps.loc[comps.etav_mv > 0, 'saving_flag'] = 1

            _calc_mvsaving(subject, comps, end_year=end_year, top=3)
            _calc_mvsaving(subject, comps, end_year=end_year, top=4)
            _calc_mvsaving(subject, comps, end_year=end_year, top=5)

        return subject.fillna(0)


def main():
    use_cols = ['PIN', 'OVACLS', 'NEIGHBORHOOD', 'TOWN', 'AGE', 'GPS_LAT', 'GPS_LON',
                'CURRENT_TOTAL', 'CURRENT_BUILDING', 'BUILDING_SQ_FT', 'CURRENT_LAND',
                'LAND_SQ_FT', 'BLDG_SF', 'SALE_DATE', 'SALE_AMOUNT', 'EXT_DESC', 'BSMT_DESC', 'RES_TYPE']

    data = pd.read_csv('www_zipappeal2018099.csv', usecols=use_cols, dtype={'PIN': 'str', 39: 'str', 43: 'str', 46: 'str'})

    data['EXT_NUM'] = data.EXT_DESC.apply(lambda x: EXTERIOR.get(x, 0))
    data['BSMT_NUM'] = data.BSMT_DESC.apply(lambda x: BASEMENT.get(x, 0))
    data['RES_NUM'] = data.RES_TYPE.apply(lambda x: RES_TYPE.get(x, 0))

    data['price_bld'] = 0.0
    data.loc[data.BUILDING_SQ_FT != 0, 'price_bld'] = data.CURRENT_BUILDING / data.BUILDING_SQ_FT

    data['price_lot'] = 0.0
    data.loc[data.LAND_SQ_FT != 0, 'price_lot'] = data.CURRENT_LAND / data.LAND_SQ_FT
    data.SALE_DATE = pd.to_datetime(data.SALE_DATE, format='%Y-%m-%d', errors='coerce')

    data['newsaving'] = 0
    data.loc[data.SALE_DATE > '01-01-{}'.format(start_year), 'newsaving'] = \
        ((data.CURRENT_TOTAL * 10) - data.SALE_AMOUNT) * EQUAL_RATE * TAX_RATE / 10
    data.loc[data.newsaving < 0, 'newsaving'] = 0

    townships = data.TOWN.unique().tolist()
    for town in townships:
        if town in [25, 72]:
            continue

        start = timeit.default_timer()
        print 'Start doing for town', town

        pool = multiprocessing.Pool(3)
        df = data[data.TOWN == 25].reset_index(drop=True)
        jobs = [pool.apply_async(calc_saving, (i, row, df)) for i, row in df.iterrows()]
        result = [job.get() for job in jobs]
        pool.close()
        output = pd.DataFrame(result)

        fillna_cols = filter(lambda x: 'saving' in x, output.columns)
        for col in fillna_cols:
            output[col] = output[col].fillna(0)

        for top in [3, 4, 5]:
            for m in ['med', 'avg']:
                cols = ['newsaving'] + ['%ssaving_%s_top%s' % (c, m, top) for c in ['av', 'mv']]
                output['highestsaving_%s_%s' % (m, top)] = output[cols].max(axis=1)

                cols = ['newsaving'] + ['%ssaving_%s_sim_top%s' % (c, m, top) for c in ['av', 'mv']]
                output['highestsaving_%s_top%s' % (m, top)] = output[cols].max(axis=1)

        # ### Store the output
        output_cols = ['PIN'] + filter(lambda x: x != 'PIN', output.columns)
        output[output_cols].to_csv('output-town-{}.csv'.format(town), index=False)

        stop = timeit.default_timer()

        print('Time for town {}: '.format(town), stop - start)


if __name__ == '__main__':
    main()


