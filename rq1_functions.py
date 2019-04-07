import pandas as pd
import os
import json
import csv
import re, datetime
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import date, timedelta

from lifelines import KaplanMeierFitter

from global_functions import *

def get_general_statistics_per_relase(df_w,relase_creation_ts_all,assignedFirst,resolvedFirst,fixedFirst):
    #Calculate general stastics per release
    df_ret=pd.DataFrame()
    df_w['release'] = df_w['release'].astype(str)

    if assignedFirst:
        assingment_column = 'first_assignment_date'
    else:
        assingment_column = 'last_assignment_date'


    if resolvedFirst:
        resolve_column = 'first_resolved_date'
    else:
        resolve_column = 'last_resolved_date'

    if fixedFirst:
        fix_column = 'first_fixed_date'
    else:
        fix_column = 'last_fixed_date'

    for version in df_w.release.unique():
        start_date = relase_creation_ts_all[version]
        start_date = pd.to_datetime(start_date)

        #Fetch the bugs of this version
        version_bugs = df_w[df_w['release']==version]
        version_bugs = trans_to_datetime(version_bugs,
                                        ['creation_time','first_assignment_date',
       'last_assignment_date', 'first_resolved_date', 'last_resolved_date',
       'first_fixed_date', 'last_fixed_date'])

        #the bug before release
        bugs_before_release = version_bugs[version_bugs['creation_time']<start_date]

        #the bug after release
        bugs_after_release = version_bugs[version_bugs['creation_time']>= start_date]

        #the bug created and resolved & before release
        bugs_created_resolved_before_release = (
            version_bugs[(version_bugs['creation_time'] < start_date) &
                        (version_bugs[resolve_column]<start_date)]
        )

        #the bug created and fixed before release
        bugs_created_fixed_before_release = (
            version_bugs[(version_bugs['creation_time'] < start_date) &
                        (version_bugs[fix_column]<start_date)]
        )

        #the bug created before release and resolved  after release
        bugs_created_before_resolved_after_release = (
            version_bugs[(version_bugs['creation_time'] < start_date) &
                        (version_bugs[resolve_column] > start_date)]
        )

        #the bug created before release and fixed after release
        bugs_created_before_fixed_after_release = (
            version_bugs[(version_bugs['creation_time'] < start_date) &
                        (version_bugs[fix_column] > start_date)]
        )

        #the bug created after release  and resolved
        bugs_created_after_relase_resolved = (
            version_bugs[(version_bugs['creation_time'] >= start_date) &
                        (version_bugs['is_resolved'] > 0)]
        )

        #the bug created after release  and fixed
        bugs_created_after_release_fixed = (
            version_bugs[(version_bugs['creation_time'] >= start_date) &
                        (version_bugs['is_fixed'] > 0)]
        )

        before_release = len(bugs_before_release.id.unique())
        RBRB= len(bugs_created_resolved_before_release.id.unique())
        RBFB = len(bugs_created_fixed_before_release.id.unique())
        if before_release!=0 and RBRB!=0:
            ratio1=RBRB/before_release
            ratio2=RBFB/RBRB

        after_release = len(bugs_after_release.id.unique())
        RARA = len(bugs_created_after_relase_resolved.id.unique())
        RAFA = len(bugs_created_after_release_fixed.id.unique())
        if after_release!=0 and RARA!=0:
            ratio3=RARA/after_release
            ratio4=RAFA/RARA
        else:
            ratio3 = 0
            ratio4 = 0

        df_ret=df_ret.append({"version": version ,
                           "total": len(version_bugs.id.unique()) ,
                           'before_release': before_release,
                           'after_release':after_release,
                           'RBRB':RBRB,
                           'RBFB':RBFB,
                           'RBRA':len(bugs_created_before_resolved_after_release.id.unique()),
                           'RBFA':len(bugs_created_before_fixed_after_release.id.unique()),
                           'RARA':RARA,
                           'RAFA':RAFA,
                           'Ratio_R_B':ratio1,
                           'Ratio_F_B':ratio2,
                           'Ratio_R_A':ratio3,
                           'Ratio_F_A':ratio4},
                         ignore_index=True)
    return df_ret

def get_monthly_stats(df_work,relase_creation_ts_all,assignedFirst,resolvedFirst,fixedFirst):
    df_work = trans_to_datetime(df_work,['creation_time','first_assignment_date',
       'last_assignment_date', 'first_resolved_date', 'last_resolved_date',
       'first_fixed_date', 'last_fixed_date'])
    df_work['release'] = df_work['release'].astype(str)
    mon_stats = pd.DataFrame()

    if assignedFirst:
        assingment_column = 'first_assignment_date'
    else:
        assingment_column = 'last_assignment_date'


    if resolvedFirst:
        resolve_column = 'first_resolved_date'
    else:
        resolve_column = 'last_resolved_date'

    if fixedFirst:
        fix_column = 'first_fixed_date'
    else:
        fix_column = 'last_fixed_date'

    for release in sorted(df_work.release.unique().tolist()):
        release = str(release)
        release_date = relase_creation_ts_all[release]
        release_date = pd.to_datetime(release_date)

        dfw = df_work[df_work['release']==release]
        dfw = trans_to_datetime(dfw,['creation_time','first_assignment_date',
       'last_assignment_date', 'first_resolved_date', 'last_resolved_date',
       'first_fixed_date', 'last_fixed_date'])

        release_events = []
        release_events.append(dfw.creation_time.min())
        release_events.append(dfw.first_assignment_date.min())
        release_events.append(dfw.first_resolved_date.min())
        release_events.append(dfw.first_fixed_date.min())
        end_inspection = min(release_events)

        month=-1
        while True:
            month+=1
            end_period = release_date-timedelta(30*month)
            if end_period < end_inspection:
                break
            start_period = release_date-timedelta(30*(month+1))

            print('Month:'+str(month)+' - '+str(start_period)+'->'+str(end_period)+' before ')

            bugs_reported = (
                dfw[(dfw['creation_time']>=start_period) &
                    (dfw['creation_time']<end_period)]
            )

            bugs_reported = bugs_reported[['id','release','Product']].drop_duplicates()
            bugs_reported['type'] = 'reported'

            bugs_assigned = (
                dfw[(dfw[assingment_column]>=start_period) &
                    (dfw[assingment_column]<end_period)]
            )
            bugs_assigned = bugs_assigned[['id','release','Product']].drop_duplicates()
            bugs_assigned['type'] = 'assigned'

            bugs_resolved = (
                dfw[(dfw[resolve_column]>=start_period) &
                    (dfw[resolve_column]<end_period)]
            )
            bugs_resolved = bugs_resolved[['id','release','Product']].drop_duplicates()
            bugs_resolved['type'] = 'resolved'

            bugs_fixed = (
                dfw[(dfw[fix_column]>=start_period) &
                    (dfw[fix_column]<end_period)]
            )
            bugs_fixed = bugs_fixed[['id','release','Product']].drop_duplicates()
            bugs_fixed['type'] = 'fixed'


            all_dfs = pd.DataFrame()
            all_dfs = all_dfs.append(bugs_reported, ignore_index=True)
            all_dfs = all_dfs.append(bugs_resolved, ignore_index=True)
            all_dfs = all_dfs.append(bugs_assigned, ignore_index=True)
            all_dfs = all_dfs.append(bugs_fixed, ignore_index=True)
            all_dfs['start_period'] = start_period
            all_dfs['end_period'] = end_period
            all_dfs['month'] = month+1
            all_dfs['period'] = 'before'

            mon_stats = mon_stats.append(all_dfs,ignore_index=True)

        release_events = []
        release_events.append(dfw.creation_time.max())
        release_events.append(dfw.last_assignment_date.max())
        release_events.append(dfw.last_resolved_date.max())
        release_events.append(dfw.last_fixed_date.max())

        end_inspection = max(release_events)

        month=-1
        while True:
            month+=1
            start_period = release_date+timedelta(30*month)
            if start_period > datetime.datetime.now() or start_period > end_inspection:
                break

            end_period = release_date+timedelta(30*(month+1))
            print('Month:'+str(month)+' - '+str(start_period)+'->'+str(end_period)+' after ')

            bugs_reported = (
                dfw[(dfw['creation_time']>=start_period) &
                    (dfw['creation_time']<end_period)]
            )

            bugs_reported = bugs_reported[['id','release','Product']].drop_duplicates()
            bugs_reported['type'] = 'reported'

            bugs_resolved = (
                dfw[(dfw[resolve_column]>=start_period) &
                    (dfw[resolve_column]<end_period)]
            )
            bugs_resolved = bugs_resolved[['id','release','Product']].drop_duplicates()
            bugs_resolved['type'] = 'resolved'

            bugs_assigned = (
                dfw[(dfw[assingment_column]>=start_period) &
                    (dfw[assingment_column]<end_period)]
            )
            bugs_assigned = bugs_assigned[['id','release','Product']].drop_duplicates()
            bugs_assigned['type'] = 'assigned'

            bugs_fixed = (
                dfw[(dfw[fix_column]>=start_period) &
                    (dfw[fix_column]<end_period)]
            )
            bugs_fixed = bugs_fixed[['id','release','Product']].drop_duplicates()
            bugs_fixed['type'] = 'fixed'


            all_dfs = pd.DataFrame()
            all_dfs = all_dfs.append(bugs_reported, ignore_index=True)
            all_dfs = all_dfs.append(bugs_resolved, ignore_index=True)
            all_dfs = all_dfs.append(bugs_assigned, ignore_index=True)
            all_dfs = all_dfs.append(bugs_fixed, ignore_index=True)
            all_dfs['start_period'] = start_period
            all_dfs['end_period'] = end_period
            all_dfs['month'] = month+1
            all_dfs['period'] = 'after'

            mon_stats = mon_stats.append(all_dfs,ignore_index=True)
    return mon_stats


def get_general_statistics_per_relase_per_severity(df_w,assignedFirst,resolvedFirst,fixedFirst):
    relase_creation_ts_all = get_release_dates()
    #Calculate general stastics per release
    df_ret=pd.DataFrame()
    df_w['release'] = df_w['release'].astype(str)

    if assignedFirst:
        assingment_column = 'first_assignment_date'
    else:
        assingment_column = 'last_assignment_date'


    if resolvedFirst:
        resolve_column = 'first_resolved_date'
    else:
        resolve_column = 'last_resolved_date'

    if fixedFirst:
        fix_column = 'first_fixed_date'
    else:
        fix_column = 'last_fixed_date'


    groups_df = df_w[['release','severity']].drop_duplicates()#,'priority'
    for index,row in groups_df.iterrows():
        version = row['release']
        severity = row['severity']
        #priority = row['priority']

        start_date = relase_creation_ts_all[version]
        start_date = pd.to_datetime(start_date)

        #Fetch the bugs of this version
        version_bugs = df_w[(df_w['release']==version) &
                           (df_w['severity']==severity)]
#        &                  (df_w['priority']==priority)]
        version_bugs = trans_to_datetime(version_bugs,
                                        ['creation_time','first_assignment_date',
       'last_assignment_date', 'first_resolved_date', 'last_resolved_date',
       'first_fixed_date', 'last_fixed_date'])

        #the bug before release
        bugs_before_release = version_bugs[version_bugs['creation_time']<start_date]

        #the bug after release
        bugs_after_release = version_bugs[version_bugs['creation_time']>= start_date]

        #the bug created and resolved & before release
        bugs_created_resolved_before_release = (
            version_bugs[(version_bugs['creation_time'] < start_date) &
                        (version_bugs[resolve_column]<start_date)]
        )

        #the bug created and fixed before release
        bugs_created_fixed_before_release = (
            version_bugs[(version_bugs['creation_time'] < start_date) &
                        (version_bugs[fix_column]<start_date)]
        )

        #the bug created before release and resolved  after release
        bugs_created_before_resolved_after_release = (
            version_bugs[(version_bugs['creation_time'] < start_date) &
                        (version_bugs[resolve_column] > start_date)]
        )

        #the bug created before release and fixed after release
        bugs_created_before_fixed_after_release = (
            version_bugs[(version_bugs['creation_time'] < start_date) &
                        (version_bugs[fix_column] > start_date)]
        )

        #the bug created after release  and resolved
        bugs_created_after_relase_resolved = (
            version_bugs[(version_bugs['creation_time'] >= start_date) &
                        (version_bugs['is_resolved'] > 0)]
        )

        #the bug created after release  and fixed
        bugs_created_after_release_fixed = (
            version_bugs[(version_bugs['creation_time'] >= start_date) &
                        (version_bugs['is_fixed'] > 0)]
        )

        before_release = len(bugs_before_release.id.unique())
        RBRB= len(bugs_created_resolved_before_release.id.unique())
        RBFB = len(bugs_created_fixed_before_release.id.unique())
        if before_release!=0 and RBRB!=0:
            ratio1=RBRB/before_release
            ratio2=RBFB/RBRB

        after_release = len(bugs_after_release.id.unique())
        RARA = len(bugs_created_after_relase_resolved.id.unique())
        RAFA = len(bugs_created_after_release_fixed.id.unique())
        if after_release!=0 and RARA!=0:
            ratio3=RARA/after_release
            ratio4=RAFA/RARA
        else:
            ratio3 = 0
            ratio4 = 0

        df_ret=df_ret.append({"version": version,
                              'severity':severity,
                              #'priority':priority,
                           "total": len(version_bugs.id.unique()) ,
                           'before_release': before_release,
                           'after_release':after_release,
                           'RBRB':RBRB,
                           'RBFB':RBFB,
                           'RBRA':len(bugs_created_before_resolved_after_release.id.unique()),
                           'RBFA':len(bugs_created_before_fixed_after_release.id.unique()),
                           'RARA':RARA,
                           'RAFA':RAFA,
                           'Ratio_R_B':ratio1,
                           'Ratio_F_B':ratio2,
                           'Ratio_R_A':ratio3,
                           'Ratio_F_A':ratio4},
                         ignore_index=True)
    return df_ret

def get_monthly_stats_severity(df_work,assignedFirst,resolvedFirst,fixedFirst):
    relase_creation_ts_all = get_release_dates()
    df_work = trans_to_datetime(df_work,['creation_time','first_assignment_date',
       'last_assignment_date', 'first_resolved_date', 'last_resolved_date',
       'first_fixed_date', 'last_fixed_date'])
    df_work['release'] = df_work['release'].astype(str)
    mon_stats = pd.DataFrame()

    if assignedFirst:
        assingment_column = 'first_assignment_date'
    else:
        assingment_column = 'last_assignment_date'


    if resolvedFirst:
        resolve_column = 'first_resolved_date'
    else:
        resolve_column = 'last_resolved_date'

    if fixedFirst:
        fix_column = 'first_fixed_date'
    else:
        fix_column = 'last_fixed_date'

    groups_df = df_work[['release','severity']].drop_duplicates()#,'priority'
    for index,row in groups_df.iterrows():
        release = row['release']
        severity = row['severity']
        #priority = row['priority']

        release = str(release)
        release_date = relase_creation_ts_all[release]
        release_date = pd.to_datetime(release_date)

        dfw = df_work[(df_work['release']==release) &
                     (df_work['severity']==severity)]
        #&            (df_work['priority']==priority)]
        dfw = trans_to_datetime(dfw,['creation_time','first_assignment_date',
       'last_assignment_date', 'first_resolved_date', 'last_resolved_date',
       'first_fixed_date', 'last_fixed_date'])

        release_events = []
        release_events.append(dfw.creation_time.min())
        release_events.append(dfw.first_assignment_date.min())
        release_events.append(dfw.first_resolved_date.min())
        release_events.append(dfw.first_fixed_date.min())

        release_events = [x for x in release_events if x is not None]
        release_events = [x for x in release_events if x is not np.nan]

        end_inspection = min(release_events)

        month=-1
        while True:
            month+=1
            end_period = release_date-timedelta(30*month)
            if end_period < end_inspection:
                break
            start_period = release_date-timedelta(30*(month+1))

            print('Month:'+str(month)+' - '+str(start_period)+'->'+str(end_period)+' before ')

            bugs_reported = (
                dfw[(dfw['creation_time']>=start_period) &
                    (dfw['creation_time']<end_period)]
            )

            bugs_reported = bugs_reported[['id','release','Product']].drop_duplicates()
            bugs_reported['type'] = 'reported'

            bugs_assigned = (
                dfw[(dfw[assingment_column]>=start_period) &
                    (dfw[assingment_column]<end_period)]
            )
            bugs_assigned = bugs_assigned[['id','release','Product']].drop_duplicates()
            bugs_assigned['type'] = 'assigned'

            bugs_resolved = (
                dfw[(dfw[resolve_column]>=start_period) &
                    (dfw[resolve_column]<end_period)]
            )
            bugs_resolved = bugs_resolved[['id','release','Product']].drop_duplicates()
            bugs_resolved['type'] = 'resolved'

            bugs_fixed = (
                dfw[(dfw[fix_column]>=start_period) &
                    (dfw[fix_column]<end_period)]
            )
            bugs_fixed = bugs_fixed[['id','release','Product']].drop_duplicates()
            bugs_fixed['type'] = 'fixed'


            all_dfs = pd.DataFrame()
            all_dfs = all_dfs.append(bugs_reported, ignore_index=True)
            all_dfs = all_dfs.append(bugs_resolved, ignore_index=True)
            all_dfs = all_dfs.append(bugs_assigned, ignore_index=True)
            all_dfs = all_dfs.append(bugs_fixed, ignore_index=True)
            all_dfs['start_period'] = start_period
            all_dfs['end_period'] = end_period
            all_dfs['month'] = month+1
            all_dfs['period'] = 'before'
            all_dfs['severity'] = severity
            #all_dfs['priority'] = priority

            mon_stats = mon_stats.append(all_dfs,ignore_index=True)

        release_events = []
        release_events.append(dfw.creation_time.max())
        release_events.append(dfw.last_assignment_date.max())
        release_events.append(dfw.last_resolved_date.max())
        release_events.append(dfw.last_fixed_date.max())

        release_events = [x for x in release_events if x is not None]
        release_events = [x for x in release_events if x is not np.nan]

        end_inspection = max(release_events)

        month=-1
        while True:
            month+=1
            start_period = release_date+timedelta(30*month)
            if start_period > datetime.datetime.now() or start_period > end_inspection:
                break

            end_period = release_date+timedelta(30*(month+1))
            print('Month:'+str(month)+' - '+str(start_period)+'->'+str(end_period)+' after ')

            bugs_reported = (
                dfw[(dfw['creation_time']>=start_period) &
                    (dfw['creation_time']<end_period)]
            )

            bugs_reported = bugs_reported[['id','release','Product']].drop_duplicates()
            bugs_reported['type'] = 'reported'

            bugs_resolved = (
                dfw[(dfw[resolve_column]>=start_period) &
                    (dfw[resolve_column]<end_period)]
            )
            bugs_resolved = bugs_resolved[['id','release','Product']].drop_duplicates()
            bugs_resolved['type'] = 'resolved'

            bugs_assigned = (
                dfw[(dfw[assingment_column]>=start_period) &
                    (dfw[assingment_column]<end_period)]
            )
            bugs_assigned = bugs_assigned[['id','release','Product']].drop_duplicates()
            bugs_assigned['type'] = 'assigned'

            bugs_fixed = (
                dfw[(dfw[fix_column]>=start_period) &
                    (dfw[fix_column]<end_period)]
            )
            bugs_fixed = bugs_fixed[['id','release','Product']].drop_duplicates()
            bugs_fixed['type'] = 'fixed'


            all_dfs = pd.DataFrame()
            all_dfs = all_dfs.append(bugs_reported, ignore_index=True)
            all_dfs = all_dfs.append(bugs_resolved, ignore_index=True)
            all_dfs = all_dfs.append(bugs_assigned, ignore_index=True)
            all_dfs = all_dfs.append(bugs_fixed, ignore_index=True)
            all_dfs['start_period'] = start_period
            all_dfs['end_period'] = end_period
            all_dfs['month'] = month+1
            all_dfs['period'] = 'after'
            all_dfs['severity'] = severity
            #all_dfs['priority'] = priority

            mon_stats = mon_stats.append(all_dfs,ignore_index=True)
    return mon_stats
