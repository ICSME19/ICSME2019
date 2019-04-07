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

def get_monthly_triaging_stats_next(df_work,release_creation_ts_all,assignedFirst,resolvedFirst):
    df_work = trans_to_datetime(df_work,['creation_time','first_assignment_date',
       'last_assignment_date', 'first_resolved_date', 'last_resolved_date',
       'first_fixed_date'])

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


    for release in sorted(df_work.release.unique().tolist()):
        release = str(release)
        pred=predecessor(list(release_creation_ts_all.keys()),release)
        #print(release,pred)
        release_date = release_creation_ts_all[release]
        release_date = pd.to_datetime(release_date)

        dfw = df_work[df_work['release']==release]
        dfw = trans_to_datetime(dfw,['creation_time','first_assignment_date',
       'last_assignment_date', 'first_resolved_date', 'last_resolved_date',
       'first_fixed_date', 'last_fixed_date'])

        release_events = []
        release_events.append(dfw.creation_time.min())
        release_events.append(dfw.first_assignment_date.min())
        end_inspection = min(release_events)

        month=-1
        while month <11:
            month+=1
            end_period = release_date-timedelta(30*month)
            if end_period < end_inspection:
                print('hi break')
                break
            start_period = release_date-timedelta(30*(month+1))

            bugs_assigned = (
                dfw[(dfw['first_assignment_date']>=start_period)
                                 &
                    (dfw['first_assignment_date']<end_period)]
            )
            bugs_assigned = bugs_assigned[['id','release','Product','creation_time','first_assignment_date']].drop_duplicates()
            bugs_assigned['type'] = 'assigned'

            all_dfs = pd.DataFrame()
            all_dfs = all_dfs.append(bugs_assigned, ignore_index=True)
            all_dfs['start_period'] = start_period
            all_dfs['end_period'] = end_period
            all_dfs['month'] = month+1
            all_dfs['period'] = 'before'
            all_dfs['release'] = release
            #all_dfs['predecessor'] = str(pred)

            mon_stats = mon_stats.append(all_dfs,ignore_index=True)

    return mon_stats


def get_monthly_triaging_stats_current(df_work,release_creation_ts_all,assignedFirst,resolvedFirst):
    df_work = trans_to_datetime(df_work,['creation_time','first_assignment_date',
       'last_assignment_date', 'first_resolved_date', 'last_resolved_date',
       'first_fixed_date'])

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


    for release in sorted(df_work.release.unique().tolist()):
        release = str(release)
        pred=predecessor(list(release_creation_ts_all.keys()),release)
        #print(release,pred)
        release_date = release_creation_ts_all[release]
        release_date = pd.to_datetime(release_date)

        dfw = df_work[df_work['release']==pred]
        dfw = trans_to_datetime(dfw,['creation_time','first_assignment_date',
       'last_assignment_date', 'first_resolved_date', 'last_resolved_date',
       'first_fixed_date', 'last_fixed_date'])

        release_events = []
        release_events.append(dfw.creation_time.min())
        release_events.append(dfw.first_assignment_date.min())
        end_inspection = min(release_events)

        month=-1
        while month <11:
            month+=1
            end_period = release_date-timedelta(30*month)
            if end_period < end_inspection:
                print('hi break')
                break
            start_period = release_date-timedelta(30*(month+1))

            bugs_assigned = (
                dfw[(dfw['first_assignment_date']>=start_period)
                                 &
                    (dfw['first_assignment_date']<end_period)]
            )
            bugs_assigned = bugs_assigned[['id','release','Product','creation_time','first_assignment_date']].drop_duplicates()
            bugs_assigned['type'] = 'assigned'

            all_dfs = pd.DataFrame()
            all_dfs = all_dfs.append(bugs_assigned, ignore_index=True)
            all_dfs['start_period'] = start_period
            all_dfs['end_period'] = end_period
            all_dfs['month'] = month+1
            all_dfs['period'] = 'before'
            all_dfs['release'] = release
            all_dfs['predecessor'] = str(pred)

            mon_stats = mon_stats.append(all_dfs,ignore_index=True)

    return mon_stats

def get_monthly_fixing_stats_next(df_work,release_creation_ts_all,assignedFirst,fixedFirst):
    df_work = trans_to_datetime(df_work,['creation_time','first_assignment_date',
       'last_assignment_date', 'first_resolved_date', 'last_resolved_date',
       'first_fixed_date'])

    df_work['release'] = df_work['release'].astype(str)
    mon_stats = pd.DataFrame()


    if fixedFirst:
        fix_column = 'first_fixed_date'
    else:
        fix_column = 'last_fixed_date'


    for release in sorted(df_work.release.unique().tolist()):


        release = str(release)
        pred=predecessor(list(release_creation_ts_all.keys()),release)
        #print(release,pred)
        release_date = release_creation_ts_all[release]
        release_date = pd.to_datetime(release_date)

        dfw = df_work[df_work['release']==release]
        dfw = trans_to_datetime(dfw,['creation_time','first_assignment_date',
       'last_assignment_date', 'first_resolved_date', 'last_resolved_date',
       'first_fixed_date', 'last_fixed_date'])

        release_events = []
        release_events.append(dfw.creation_time.min())
        release_events.append(dfw[fix_column].min())
        end_inspection = min(release_events)

        month=-1
        while month <11:
            month+=1
            end_period = release_date-timedelta(30*month)
            if end_period < end_inspection:
                print('hi break')
                break
            start_period = release_date-timedelta(30*(month+1))

            bugs_assigned = (
                dfw[(dfw[fix_column]>=start_period)
                                 &
                    (dfw[fix_column]<end_period)]
            )
            bugs_assigned = bugs_assigned[['id','release','Product','creation_time','last_fixed_date']].drop_duplicates()
            bugs_assigned['type'] = 'fixed'

            all_dfs = pd.DataFrame()
            all_dfs = all_dfs.append(bugs_assigned, ignore_index=True)
            all_dfs['start_period'] = start_period
            all_dfs['end_period'] = end_period
            all_dfs['month'] = month+1
            all_dfs['period'] = 'before'
            all_dfs['release'] = release
            #all_dfs['predecessor'] = str(pred)

            mon_stats = mon_stats.append(all_dfs,ignore_index=True)

    return mon_stats

def get_monthly_fixing_stats_current(df_work,release_creation_ts_all,yearly_releases,fixedFirst,resolvedFirst):
    df_work = trans_to_datetime(df_work,['creation_time','first_assignment_date',
       'last_assignment_date', 'first_resolved_date', 'last_resolved_date',
       'first_fixed_date','last_fixed_date'])

    df_work['release'] = df_work['release'].astype(str)
    mon_stats = pd.DataFrame()


    if fixedFirst:
        fix_column = 'first_fixed_date'
    else:
        fix_column = 'last_fixed_date'



    for release in sorted(df_work.release.unique().tolist()):
        release = str(release)
        pred=predecessor(yearly_releases,release)
        #print(release,pred)
        release_date = release_creation_ts_all[release]
        release_date = pd.to_datetime(release_date)

        dfw = df_work[df_work['release']==pred]
        dfw = trans_to_datetime(dfw,['creation_time','first_assignment_date',
       'last_assignment_date', 'first_resolved_date', 'last_resolved_date',
       'first_fixed_date', 'last_fixed_date'])

        release_events = []
        release_events.append(dfw.creation_time.min())
        release_events.append(dfw[fix_column].min())
        end_inspection = min(release_events)

        month=-1
        while month <11:
            month+=1
            end_period = release_date-timedelta(30*month)
            if end_period < end_inspection:
                print('hi break')
                break
            start_period = release_date-timedelta(30*(month+1))

            #print('Month:'+str(month)+' - '+str(start_period)+'->'+str(end_period)+' before ')


            bugs_fixed = (
                dfw[(dfw[fix_column]>=start_period)
                                 &
                    (dfw[fix_column]<end_period)]
            )
            bugs_fixed = bugs_fixed[['id','release','Product','creation_time',fix_column]].drop_duplicates()
            bugs_fixed['type'] = 'fixed'




            all_dfs = pd.DataFrame()
            #all_dfs = all_dfs.append(bugs_reported, ignore_index=True)
            #all_dfs = all_dfs.append(bugs_resolved, ignore_index=True)
            all_dfs = all_dfs.append(bugs_fixed, ignore_index=True)
           # all_dfs = all_dfs.append(bugs_fixed, ignore_index=True)
            all_dfs['start_period'] = start_period
            all_dfs['end_period'] = end_period
            all_dfs['month'] = month+1
            all_dfs['period'] = 'before'
            #print(pred,release)
            all_dfs['release'] = release
            all_dfs['predecessor'] = str(pred)
            #print(all_dfs['predecessor'].unique())

            mon_stats = mon_stats.append(all_dfs,ignore_index=True)

    return mon_stats
