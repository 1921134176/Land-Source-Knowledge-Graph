#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
@author: ChengXin
@contact:1921134176@qq.com
@version: 1.0.0
@license: Apache Licence
@file: landcover.py
@time: 2021/11/3 14:04
"""
import requests
import re
from collections import Counter
import wget
from conutrykg import rasterclip
import json
import os
import multiprocessing


class landcover:
    def __init__(self):
        self.dataset = {
            'glc_1985_30m': ['..\\data\\landcover\\GLC1985\\GLC\\', '..\\data\\ChineseLandcover\\GLC1985\\'],
            'glc_1990_30m': ['..\\data\\landcover\\GLC1990\\GLC\\', '..\\data\\ChineseLandcover\\GLC1990\\'],
            'glc_1995_30m': ['..\\data\\landcover\\GLC1995\\GLC\\', '..\\data\\ChineseLandcover\\GLC1995\\'],
            'glc_2000_30m': ['..\\data\\landcover\\GLC2000\\GLC\\', '..\\data\\ChineseLandcover\\GLC2000\\'],
            'glc_2005_30m': ['..\\data\\landcover\\GLC2005\\GLC\\', '..\\data\\ChineseLandcover\\GLC2005\\'],
            'glc_2010_30m': ['..\\data\\landcover\\GLC2010\\GLC\\', '..\\data\\ChineseLandcover\\GLC2010\\'],
            'glc_2015_30m': ['..\\data\\landcover\\GLC2015\\GLC\\', '..\\data\\ChineseLandcover\\GLC2015\\'],
            'glc_2020_30m': ['..\\data\\landcover\\GLC2020\\GLC\\', '..\\data\\ChineseLandcover\\GLC2020\\'],
            'glc_2017_10m_Tinghua': ['..\\data\\landcover\\GLC2017\\GLC\\', '..\\data\\ChineseLandcover\\GLC2017\\'],
            'GlobaLand30_2000': ['..\\data\\landcover\\GlobaLand30_2000\\GLC\\',
                                 '..\\data\\ChineseLandcover\\GlobaLand30_2000\\'],
            'GlobaLand30_2010': ['..\\data\\landcover\\GlobaLand30_2010\\GLC\\',
                                 '..\\data\\ChineseLandcover\\GlobaLand30_2010\\'],
            'GlobaLand30_2020': ['..\\data\\landcover\\GlobaLand30_2020\\GLC\\',
                                 '..\\data\\ChineseLandcover\\GlobaLand30_2020\\'],
            'WSF2019': ['..\\data\\landcover\\WSF2019\\WSF\\', '..\\data\\ChineseLandcover\\WSF2019\\'],
            'WSF2015': ['..\\data\\landcover\\WSF2015\\WSF\\', '..\\data\\ChineseLandcover\\WSF2015\\']
        }
        self.datasetLink = {
            'glc_1985_30m': {'author': '刘良云', 'company': '中国科学院空天信息创新研究院', '地域范围': '全球陆地区域',
                             '共享方式': '公开共享', '数据格式': 'GeoTiff', '数据类型': '栅格', '数据内容': '土地覆盖分类',
                             'link': 'https://data.casearth.cn/sdo/detail/6123651428a58f70c2a51e42'},
            'glc_1990_30m': {'author': '刘良云', 'company': '中国科学院空天信息创新研究院', '地域范围': '全球陆地区域',
                             '共享方式': '公开共享', '数据格式': 'GeoTiff', '数据类型': '栅格', '数据内容': '土地覆盖分类',
                             'link': 'https://data.casearth.cn/sdo/detail/6123651428a58f70c2a51e43'},
            'glc_1995_30m': {'author': '刘良云', 'company': '中国科学院空天信息创新研究院', '地域范围': '全球陆地区域',
                             '共享方式': '公开共享', '数据格式': 'GeoTiff', '数据类型': '栅格', '数据内容': '土地覆盖分类',
                             'link': 'https://data.casearth.cn/sdo/detail/6123651428a58f70c2a51e44'},
            'glc_2000_30m': {'author': '刘良云', 'company': '中国科学院空天信息创新研究院', '地域范围': '全球陆地区域',
                             '共享方式': '公开共享', '数据格式': 'GeoTiff', '数据类型': '栅格', '数据内容': '土地覆盖分类',
                             'link': 'https://data.casearth.cn/sdo/detail/6123651428a58f70c2a51e45'},
            'glc_2005_30m': {'author': '刘良云', 'company': '中国科学院空天信息创新研究院', '地域范围': '全球陆地区域',
                             '共享方式': '公开共享', '数据格式': 'GeoTiff', '数据类型': '栅格', '数据内容': '土地覆盖分类',
                             'link': 'https://data.casearth.cn/sdo/detail/6123651428a58f70c2a51e46'},
            'glc_2010_30m': {'author': '刘良云', 'company': '中国科学院空天信息创新研究院', '地域范围': '全球陆地区域',
                             '共享方式': '公开共享', '数据格式': 'GeoTiff', '数据类型': '栅格', '数据内容': '土地覆盖分类',
                             'link': 'https://data.casearth.cn/sdo/detail/6123651428a58f70c2a51e47'},
            'glc_2015_30m': {'author': '刘良云', 'company': '中国科学院空天信息创新研究院', '地域范围': '全球陆地区域',
                             '共享方式': '公开共享', '数据格式': 'GeoTiff', '数据类型': '栅格', '数据内容': '土地覆盖分类',
                             'link': 'https://data.casearth.cn/sdo/detail/6123651428a58f70c2a51e48'},
            'glc_2020_30m': {'author': '刘良云', 'company': '中国科学院空天信息创新研究院', '地域范围': '全球陆地区域',
                             '共享方式': '公开共享', '数据格式': 'GeoTiff', '数据类型': '栅格', '数据内容': '土地覆盖分类',
                             'link': 'https://data.casearth.cn/sdo/detail/6123651428a58f70c2a51e49'},
            'glc_2017_10m_Tinghua': {'author': '宫鹏', 'company': '清华大学', '地域范围': '全球陆地区域',
                                     '共享方式': '公开共享', '数据格式': 'GeoTiff', '数据类型': '栅格', '数据内容': '土地覆盖分类',
                                     'link': 'http://data.ess.tsinghua.edu.cn/fromglc10_2017v01.html'},
            'GlobaLand30_2000': {'author': '陈军', 'company': '自然资源部', '地域范围': '全球陆地区域',
                                 '共享方式': '公开共享', '数据格式': 'GeoTiff', '数据类型': '栅格', '数据内容': '土地覆盖分类',
                                 'link': 'http://www.globeland30.org/'},
            'GlobaLand30_2010': {'author': '陈军', 'company': '自然资源部', '地域范围': '全球陆地区域',
                                 '共享方式': '公开共享', '数据格式': 'GeoTiff', '数据类型': '栅格', '数据内容': '土地覆盖分类',
                                 'link': 'http://www.globeland30.org/', 'Overall_Accuracy': 0.835, 'Kappa': 0.78},
            'GlobaLand30_2020': {'author': '陈军', 'company': '自然资源部', '地域范围': '全球陆地区域',
                                 '共享方式': '公开共享', '数据格式': 'GeoTiff', '数据类型': '栅格', '数据内容': '土地覆盖分类',
                                 'link': 'http://www.globeland30.org/', 'Overall_Accuracy': 0.8572, 'Kappa': 0.82},
        }
        self.label = {
            'glc_1985_30m': ['dataset', 'Landcover_Dataset'],
            'glc_1990_30m': ['dataset', 'Landcover_Dataset'],
            'glc_1995_30m': ['dataset', 'Landcover_Dataset'],
            'glc_2000_30m': ['dataset', 'Landcover_Dataset'],
            'glc_2005_30m': ['dataset', 'Landcover_Dataset'],
            'glc_2010_30m': ['dataset', 'Landcover_Dataset'],
            'glc_2015_30m': ['dataset', 'Landcover_Dataset'],
            'glc_2020_30m': ['dataset', 'Landcover_Dataset'],
            'glc_2017_10m_Tinghua': ['dataset', 'Landcover_Dataset'],
            'GlobaLand30_2000': ['dataset', 'Landcover_Dataset'],
            'GlobaLand30_2010': ['dataset', 'Landcover_Dataset'],
            'GlobaLand30_2020': ['dataset', 'Landcover_Dataset']}
        self.resolution = {'glc_2020_30m': 30,
                           'glc_2015_30m': 30,
                           'glc_2010_30m': 30,
                           'glc_2005_30m': 30,
                           'glc_2000_30m': 30,
                           'glc_1995_30m': 30,
                           'glc_1990_30m': 30,
                           'glc_1985_30m': 30,
                           'glc_2017_10m_Tinghua': 10,
                           'GlobaLand30_2000': 30,
                           'GlobaLand30_2010': 30,
                           'GlobaLand30_2020': 30}
        self.count = {'glc_2020_30m': Counter({10: 0, 11: 0, 12: 0, 20: 0, 51: 0, 52: 0, 61: 0, 62: 0,
                                               71: 0, 72: 0, 81: 0, 82: 0, 91: 0, 92: 0, 120: 0, 121: 0,
                                               122: 0, 130: 0, 140: 0, 150: 0, 152: 0, 153: 0, 180: 0,
                                               190: 0, 200: 0, 201: 0, 202: 0, 210: 0, 220: 0, 250: 0}),
                      'glc_2015_30m': Counter({10: 0, 11: 0, 12: 0, 20: 0, 51: 0, 52: 0, 61: 0, 62: 0,
                                               71: 0, 72: 0, 81: 0, 82: 0, 91: 0, 92: 0, 120: 0, 121: 0,
                                               122: 0, 130: 0, 140: 0, 150: 0, 152: 0, 153: 0, 180: 0,
                                               190: 0, 200: 0, 201: 0, 202: 0, 210: 0, 220: 0, 250: 0}),
                      'glc_2010_30m': Counter({10: 0, 11: 0, 12: 0, 20: 0, 51: 0, 52: 0, 61: 0, 62: 0,
                                               71: 0, 72: 0, 81: 0, 82: 0, 91: 0, 92: 0, 120: 0, 121: 0,
                                               122: 0, 130: 0, 140: 0, 150: 0, 152: 0, 153: 0, 180: 0,
                                               190: 0, 200: 0, 201: 0, 202: 0, 210: 0, 220: 0, 250: 0}),
                      'glc_2005_30m': Counter({10: 0, 11: 0, 12: 0, 20: 0, 51: 0, 52: 0, 61: 0, 62: 0,
                                               71: 0, 72: 0, 81: 0, 82: 0, 91: 0, 92: 0, 120: 0, 121: 0,
                                               122: 0, 130: 0, 140: 0, 150: 0, 152: 0, 153: 0, 180: 0,
                                               190: 0, 200: 0, 201: 0, 202: 0, 210: 0, 220: 0, 250: 0}),
                      'glc_2000_30m': Counter({10: 0, 11: 0, 12: 0, 20: 0, 51: 0, 52: 0, 61: 0, 62: 0,
                                               71: 0, 72: 0, 81: 0, 82: 0, 91: 0, 92: 0, 120: 0, 121: 0,
                                               122: 0, 130: 0, 140: 0, 150: 0, 152: 0, 153: 0, 180: 0,
                                               190: 0, 200: 0, 201: 0, 202: 0, 210: 0, 220: 0, 250: 0}),
                      'glc_1995_30m': Counter({10: 0, 11: 0, 12: 0, 20: 0, 51: 0, 52: 0, 61: 0, 62: 0,
                                               71: 0, 72: 0, 81: 0, 82: 0, 91: 0, 92: 0, 120: 0, 121: 0,
                                               122: 0, 130: 0, 140: 0, 150: 0, 152: 0, 153: 0, 180: 0,
                                               190: 0, 200: 0, 201: 0, 202: 0, 210: 0, 220: 0, 250: 0}),
                      'glc_1990_30m': Counter({10: 0, 11: 0, 12: 0, 20: 0, 51: 0, 52: 0, 61: 0, 62: 0,
                                               71: 0, 72: 0, 81: 0, 82: 0, 91: 0, 92: 0, 120: 0, 121: 0,
                                               122: 0, 130: 0, 140: 0, 150: 0, 152: 0, 153: 0, 180: 0,
                                               190: 0, 200: 0, 201: 0, 202: 0, 210: 0, 220: 0, 250: 0}),
                      'glc_1985_30m': Counter({10: 0, 11: 0, 12: 0, 20: 0, 51: 0, 52: 0, 61: 0, 62: 0,
                                               71: 0, 72: 0, 81: 0, 82: 0, 91: 0, 92: 0, 120: 0, 121: 0,
                                               122: 0, 130: 0, 140: 0, 150: 0, 152: 0, 153: 0, 180: 0,
                                               190: 0, 200: 0, 201: 0, 202: 0, 210: 0, 220: 0, 250: 0}),
                      'glc_2017_10m_Tinghua': Counter({10: 0, 20: 0, 30: 0, 40: 0, 50: 0, 60: 0, 70: 0,
                                                       80: 0, 90: 0, 100: 0}),
                      'GlobaLand30_2000': Counter({10: 0, 20: 0, 30: 0, 40: 0, 50: 0, 60: 0, 70: 0, 80: 0,
                                                   90: 0, 100: 0}),
                      'GlobaLand30_2010': Counter({10: 0, 20: 0, 30: 0, 40: 0, 50: 0, 60: 0, 70: 0, 80: 0,
                                                   90: 0, 100: 0}),
                      'GlobaLand30_2020': Counter({10: 0, 20: 0, 30: 0, 40: 0, 50: 0, 60: 0, 70: 0, 80: 0,
                                                   90: 0, 100: 0})
                      }
        self.value_to_className = {'glc_2020_30m': {10: 'Rainfed cropland',
                                                    11: 'Herbaceous cover',
                                                    12: 'Tree or shrub cover (Orchard)',
                                                    20: 'Irrigated cropland',
                                                    51: 'Open evergreen broadleaved forest',
                                                    52: 'Closed evergreen broadleaved forest',
                                                    61: 'Open deciduous broadleaved forest (0.15<fc<0.4)',
                                                    62: 'Closed deciduous broadleaved forest (fc>0.4)',
                                                    71: 'Open evergreen needle-leaved forest (0.15< fc <0.4)',
                                                    72: 'Closed evergreen needle-leaved forest (fc >0.4)',
                                                    81: 'Open deciduous needle-leaved forest (0.15< fc <0.4)',
                                                    82: 'Closed deciduous needle-leaved forest (fc >0.4)',
                                                    91: 'Open mixed leaf forest (broadleaved and needle-leaved)',
                                                    92: 'Closed mixed leaf forest (broadleaved and needle-leaved)',
                                                    120: 'Shrubland',
                                                    121: 'Evergreen shrubland',
                                                    122: 'Deciduous shrubland',
                                                    130: 'Grassland',
                                                    140: 'Lichens and mosses',
                                                    150: 'Sparse vegetation (fc<0.15)',
                                                    152: 'Sparse shrubland (fc<0.15)',
                                                    153: 'Sparse herbaceous (fc<0.15)',
                                                    180: 'Wetlands',
                                                    190: 'Impervious surfaces',
                                                    200: 'Bare areas',
                                                    201: 'Consolidated bare areas',
                                                    202: 'Unconsolidated bare areas',
                                                    210: 'Water body',
                                                    220: 'Permanent ice and snow',
                                                    250: 'Filled value'},
                                   'glc_2015_30m': {10: 'Rainfed cropland',
                                                    11: 'Herbaceous cover',
                                                    12: 'Tree or shrub cover (Orchard)',
                                                    20: 'Irrigated cropland',
                                                    51: 'Open evergreen broadleaved forest',
                                                    52: 'Closed evergreen broadleaved forest',
                                                    61: 'Open deciduous broadleaved forest (0.15<fc<0.4)',
                                                    62: 'Closed deciduous broadleaved forest (fc>0.4)',
                                                    71: 'Open evergreen needle-leaved forest (0.15< fc <0.4)',
                                                    72: 'Closed evergreen needle-leaved forest (fc >0.4)',
                                                    81: 'Open deciduous needle-leaved forest (0.15< fc <0.4)',
                                                    82: 'Closed deciduous needle-leaved forest (fc >0.4)',
                                                    91: 'Open mixed leaf forest (broadleaved and needle-leaved)',
                                                    92: 'Closed mixed leaf forest (broadleaved and needle-leaved)',
                                                    120: 'Shrubland',
                                                    121: 'Evergreen shrubland',
                                                    122: 'Deciduous shrubland',
                                                    130: 'Grassland',
                                                    140: 'Lichens and mosses',
                                                    150: 'Sparse vegetation (fc<0.15)',
                                                    152: 'Sparse shrubland (fc<0.15)',
                                                    153: 'Sparse herbaceous (fc<0.15)',
                                                    180: 'Wetlands',
                                                    190: 'Impervious surfaces',
                                                    200: 'Bare areas',
                                                    201: 'Consolidated bare areas',
                                                    202: 'Unconsolidated bare areas',
                                                    210: 'Water body',
                                                    220: 'Permanent ice and snow',
                                                    250: 'Filled value'},
                                   'glc_2010_30m': {10: 'Rainfed cropland',
                                                    11: 'Herbaceous cover',
                                                    12: 'Tree or shrub cover (Orchard)',
                                                    20: 'Irrigated cropland',
                                                    51: 'Open evergreen broadleaved forest',
                                                    52: 'Closed evergreen broadleaved forest',
                                                    61: 'Open deciduous broadleaved forest (0.15<fc<0.4)',
                                                    62: 'Closed deciduous broadleaved forest (fc>0.4)',
                                                    71: 'Open evergreen needle-leaved forest (0.15< fc <0.4)',
                                                    72: 'Closed evergreen needle-leaved forest (fc >0.4)',
                                                    81: 'Open deciduous needle-leaved forest (0.15< fc <0.4)',
                                                    82: 'Closed deciduous needle-leaved forest (fc >0.4)',
                                                    91: 'Open mixed leaf forest (broadleaved and needle-leaved)',
                                                    92: 'Closed mixed leaf forest (broadleaved and needle-leaved)',
                                                    120: 'Shrubland',
                                                    121: 'Evergreen shrubland',
                                                    122: 'Deciduous shrubland',
                                                    130: 'Grassland',
                                                    140: 'Lichens and mosses',
                                                    150: 'Sparse vegetation (fc<0.15)',
                                                    152: 'Sparse shrubland (fc<0.15)',
                                                    153: 'Sparse herbaceous (fc<0.15)',
                                                    180: 'Wetlands',
                                                    190: 'Impervious surfaces',
                                                    200: 'Bare areas',
                                                    201: 'Consolidated bare areas',
                                                    202: 'Unconsolidated bare areas',
                                                    210: 'Water body',
                                                    220: 'Permanent ice and snow',
                                                    250: 'Filled value'},
                                   'glc_2005_30m': {10: 'Rainfed cropland',
                                                    11: 'Herbaceous cover',
                                                    12: 'Tree or shrub cover (Orchard)',
                                                    20: 'Irrigated cropland',
                                                    51: 'Open evergreen broadleaved forest',
                                                    52: 'Closed evergreen broadleaved forest',
                                                    61: 'Open deciduous broadleaved forest (0.15<fc<0.4)',
                                                    62: 'Closed deciduous broadleaved forest (fc>0.4)',
                                                    71: 'Open evergreen needle-leaved forest (0.15< fc <0.4)',
                                                    72: 'Closed evergreen needle-leaved forest (fc >0.4)',
                                                    81: 'Open deciduous needle-leaved forest (0.15< fc <0.4)',
                                                    82: 'Closed deciduous needle-leaved forest (fc >0.4)',
                                                    91: 'Open mixed leaf forest (broadleaved and needle-leaved)',
                                                    92: 'Closed mixed leaf forest (broadleaved and needle-leaved)',
                                                    120: 'Shrubland',
                                                    121: 'Evergreen shrubland',
                                                    122: 'Deciduous shrubland',
                                                    130: 'Grassland',
                                                    140: 'Lichens and mosses',
                                                    150: 'Sparse vegetation (fc<0.15)',
                                                    152: 'Sparse shrubland (fc<0.15)',
                                                    153: 'Sparse herbaceous (fc<0.15)',
                                                    180: 'Wetlands',
                                                    190: 'Impervious surfaces',
                                                    200: 'Bare areas',
                                                    201: 'Consolidated bare areas',
                                                    202: 'Unconsolidated bare areas',
                                                    210: 'Water body',
                                                    220: 'Permanent ice and snow',
                                                    250: 'Filled value'},
                                   'glc_2000_30m': {10: 'Rainfed cropland',
                                                    11: 'Herbaceous cover',
                                                    12: 'Tree or shrub cover (Orchard)',
                                                    20: 'Irrigated cropland',
                                                    51: 'Open evergreen broadleaved forest',
                                                    52: 'Closed evergreen broadleaved forest',
                                                    61: 'Open deciduous broadleaved forest (0.15<fc<0.4)',
                                                    62: 'Closed deciduous broadleaved forest (fc>0.4)',
                                                    71: 'Open evergreen needle-leaved forest (0.15< fc <0.4)',
                                                    72: 'Closed evergreen needle-leaved forest (fc >0.4)',
                                                    81: 'Open deciduous needle-leaved forest (0.15< fc <0.4)',
                                                    82: 'Closed deciduous needle-leaved forest (fc >0.4)',
                                                    91: 'Open mixed leaf forest (broadleaved and needle-leaved)',
                                                    92: 'Closed mixed leaf forest (broadleaved and needle-leaved)',
                                                    120: 'Shrubland',
                                                    121: 'Evergreen shrubland',
                                                    122: 'Deciduous shrubland',
                                                    130: 'Grassland',
                                                    140: 'Lichens and mosses',
                                                    150: 'Sparse vegetation (fc<0.15)',
                                                    152: 'Sparse shrubland (fc<0.15)',
                                                    153: 'Sparse herbaceous (fc<0.15)',
                                                    180: 'Wetlands',
                                                    190: 'Impervious surfaces',
                                                    200: 'Bare areas',
                                                    201: 'Consolidated bare areas',
                                                    202: 'Unconsolidated bare areas',
                                                    210: 'Water body',
                                                    220: 'Permanent ice and snow',
                                                    250: 'Filled value'},
                                   'glc_1995_30m': {10: 'Rainfed cropland',
                                                    11: 'Herbaceous cover',
                                                    12: 'Tree or shrub cover (Orchard)',
                                                    20: 'Irrigated cropland',
                                                    51: 'Open evergreen broadleaved forest',
                                                    52: 'Closed evergreen broadleaved forest',
                                                    61: 'Open deciduous broadleaved forest (0.15<fc<0.4)',
                                                    62: 'Closed deciduous broadleaved forest (fc>0.4)',
                                                    71: 'Open evergreen needle-leaved forest (0.15< fc <0.4)',
                                                    72: 'Closed evergreen needle-leaved forest (fc >0.4)',
                                                    81: 'Open deciduous needle-leaved forest (0.15< fc <0.4)',
                                                    82: 'Closed deciduous needle-leaved forest (fc >0.4)',
                                                    91: 'Open mixed leaf forest (broadleaved and needle-leaved)',
                                                    92: 'Closed mixed leaf forest (broadleaved and needle-leaved)',
                                                    120: 'Shrubland',
                                                    121: 'Evergreen shrubland',
                                                    122: 'Deciduous shrubland',
                                                    130: 'Grassland',
                                                    140: 'Lichens and mosses',
                                                    150: 'Sparse vegetation (fc<0.15)',
                                                    152: 'Sparse shrubland (fc<0.15)',
                                                    153: 'Sparse herbaceous (fc<0.15)',
                                                    180: 'Wetlands',
                                                    190: 'Impervious surfaces',
                                                    200: 'Bare areas',
                                                    201: 'Consolidated bare areas',
                                                    202: 'Unconsolidated bare areas',
                                                    210: 'Water body',
                                                    220: 'Permanent ice and snow',
                                                    250: 'Filled value'},
                                   'glc_1990_30m': {10: 'Rainfed cropland',
                                                    11: 'Herbaceous cover',
                                                    12: 'Tree or shrub cover (Orchard)',
                                                    20: 'Irrigated cropland',
                                                    51: 'Open evergreen broadleaved forest',
                                                    52: 'Closed evergreen broadleaved forest',
                                                    61: 'Open deciduous broadleaved forest (0.15<fc<0.4)',
                                                    62: 'Closed deciduous broadleaved forest (fc>0.4)',
                                                    71: 'Open evergreen needle-leaved forest (0.15< fc <0.4)',
                                                    72: 'Closed evergreen needle-leaved forest (fc >0.4)',
                                                    81: 'Open deciduous needle-leaved forest (0.15< fc <0.4)',
                                                    82: 'Closed deciduous needle-leaved forest (fc >0.4)',
                                                    91: 'Open mixed leaf forest (broadleaved and needle-leaved)',
                                                    92: 'Closed mixed leaf forest (broadleaved and needle-leaved)',
                                                    120: 'Shrubland',
                                                    121: 'Evergreen shrubland',
                                                    122: 'Deciduous shrubland',
                                                    130: 'Grassland',
                                                    140: 'Lichens and mosses',
                                                    150: 'Sparse vegetation (fc<0.15)',
                                                    152: 'Sparse shrubland (fc<0.15)',
                                                    153: 'Sparse herbaceous (fc<0.15)',
                                                    180: 'Wetlands',
                                                    190: 'Impervious surfaces',
                                                    200: 'Bare areas',
                                                    201: 'Consolidated bare areas',
                                                    202: 'Unconsolidated bare areas',
                                                    210: 'Water body',
                                                    220: 'Permanent ice and snow',
                                                    250: 'Filled value'},
                                   'glc_1985_30m': {10: 'Rainfed cropland',
                                                    11: 'Herbaceous cover',
                                                    12: 'Tree or shrub cover (Orchard)',
                                                    20: 'Irrigated cropland',
                                                    51: 'Open evergreen broadleaved forest',
                                                    52: 'Closed evergreen broadleaved forest',
                                                    61: 'Open deciduous broadleaved forest (0.15<fc<0.4)',
                                                    62: 'Closed deciduous broadleaved forest (fc>0.4)',
                                                    71: 'Open evergreen needle-leaved forest (0.15< fc <0.4)',
                                                    72: 'Closed evergreen needle-leaved forest (fc >0.4)',
                                                    81: 'Open deciduous needle-leaved forest (0.15< fc <0.4)',
                                                    82: 'Closed deciduous needle-leaved forest (fc >0.4)',
                                                    91: 'Open mixed leaf forest (broadleaved and needle-leaved)',
                                                    92: 'Closed mixed leaf forest (broadleaved and needle-leaved)',
                                                    120: 'Shrubland',
                                                    121: 'Evergreen shrubland',
                                                    122: 'Deciduous shrubland',
                                                    130: 'Grassland',
                                                    140: 'Lichens and mosses',
                                                    150: 'Sparse vegetation (fc<0.15)',
                                                    152: 'Sparse shrubland (fc<0.15)',
                                                    153: 'Sparse herbaceous (fc<0.15)',
                                                    180: 'Wetlands',
                                                    190: 'Impervious surfaces',
                                                    200: 'Bare areas',
                                                    201: 'Consolidated bare areas',
                                                    202: 'Unconsolidated bare areas',
                                                    210: 'Water body',
                                                    220: 'Permanent ice and snow',
                                                    250: 'Filled value'},
                                   'glc_2017_10m_Tinghua': {10: 'Cropland',
                                                            20: 'Forest',
                                                            30: 'Grassland',
                                                            40: 'Shrubland',
                                                            50: 'Wetland',
                                                            60: 'Water',
                                                            70: 'Tundra',
                                                            80: 'Impervious surface',
                                                            90: 'Bareland',
                                                            100: 'Snow/Ice'},
                                   'GlobaLand30_2000': {10: 'Cultivated Land',
                                                        20: 'Forest',
                                                        30: 'Grassland',
                                                        40: 'Shrubland',
                                                        50: 'Wetland',
                                                        60: 'Water',
                                                        70: 'Tundra',
                                                        80: 'Impervious surface',
                                                        90: 'Bareland',
                                                        100: 'Snow/Ice'},
                                   'GlobaLand30_2010': {10: 'Cultivated Land',
                                                        20: 'Forest',
                                                        30: 'Grassland',
                                                        40: 'Shrubland',
                                                        50: 'Wetland',
                                                        60: 'Water',
                                                        70: 'Tundra',
                                                        80: 'Impervious surface',
                                                        90: 'Bareland',
                                                        100: 'Snow/Ice'},
                                   'GlobaLand30_2020': {10: 'Cultivated Land',
                                                        20: 'Forest',
                                                        30: 'Grassland',
                                                        40: 'Shrubland',
                                                        50: 'Wetland',
                                                        60: 'Water',
                                                        70: 'Tundra',
                                                        80: 'Impervious surface',
                                                        90: 'Bareland',
                                                        100: 'Snow/Ice'}
                                   }
        self.datasetProcessFunction = {
            'glc_2020_30m': rasterclip.glc2020Raster,
            'glc_2015_30m': rasterclip.glc2020Raster,
            'glc_2010_30m': rasterclip.glc2020Raster,
            'glc_2005_30m': rasterclip.glc2020Raster,
            'glc_2000_30m': rasterclip.glc2020Raster,
            'glc_1995_30m': rasterclip.glc2020Raster,
            'glc_1990_30m': rasterclip.glc2020Raster,
            'glc_1985_30m': rasterclip.glc2020Raster,
            'glc_2017_10m_Tinghua': rasterclip.glc2017Raster,
            'GlobaLand30_2000': rasterclip.GlobaLand2020Raster,
            'GlobaLand30_2010': rasterclip.GlobaLand2020Raster,
            'GlobaLand30_2020': rasterclip.GlobaLand2020Raster
        }

    def download_GLC_ChineseAcademyOfSciences(self):
        """
        获取下载链接，后建议使用迅雷下载
        :return:
        """
        glc2020 = 'https://data.casearth.cn/api/getAllFileListBySdoId?sdoId=6123651428a58f70c2a51e49'
        glc2015 = 'https://data.casearth.cn/api/getAllFileListBySdoId?sdoId=6123651428a58f70c2a51e48'
        glc2010 = 'https://data.casearth.cn/api/getAllFileListBySdoId?sdoId=6123651428a58f70c2a51e47'
        glc2005 = 'https://data.casearth.cn/api/getAllFileListBySdoId?sdoId=6123651428a58f70c2a51e46'
        glc2000 = 'https://data.casearth.cn/api/getAllFileListBySdoId?sdoId=6123651428a58f70c2a51e45'
        glc1995 = 'https://data.casearth.cn/api/getAllFileListBySdoId?sdoId=6123651428a58f70c2a51e44'
        glc1990 = 'https://data.casearth.cn/api/getAllFileListBySdoId?sdoId=6123651428a58f70c2a51e43'
        glc1985 = 'https://data.casearth.cn/api/getAllFileListBySdoId?sdoId=6123651428a58f70c2a51e42'
        apiList = [glc1985, glc1990, glc1995, glc2000, glc2005, glc2010, glc2015, glc2020]
        dataName = ['glc_1985_30m', 'glc_1990_30m', 'glc_1995_30m', 'glc_2000_30m',
                    'glc_2005_30m', 'glc_2010_30m', 'glc_2015_30m', 'glc_2020_30m']
        # 并行下载
        pool = multiprocessing.Pool(8)
        for index, api in enumerate(apiList):
            pool.apply_async(downDataset_GLC, args=(api, dataName[index]))
        pool.close()
        pool.join()

    def download_GLC2017_Tinghua(self):
        # 获取下载连接并保存、建议使用迅雷下载
        html = 'http://data.ess.tsinghua.edu.cn/fromglc10_2017v01.html'
        r = requests.get(html)
        data = r.text
        link_list = re.findall(r"(?<=href=\").+?(?=\")|(?<=href=\').+?(?=\')", data)[1:-1]
        f1 = open(self.dataset['glc_2017_10m_Tinghua'][0][:-4] + 'downURL.txt', 'w')
        for url in link_list:
            f1.writelines(url)
            f1.write('\n')
            # wget.download(url, out=self.dataset['glc_2017_10m_Tinghua'][0] + wget.filename_from_url(url))
        f1.close()

    def download_WSF2019(self):
        # 2019年全球居民地数据集
        html = 'https://download.geoservice.dlr.de/WSF2019/files/'
        r = requests.get(html)
        data = r.text
        link_list = re.findall(r"(?<=href=\").+?(?=\")|(?<=href=\').+?(?=\')", data)[1:-1]
        f1 = open(self.dataset['WSF2019'][0][:-4] + 'downURL.txt', 'w')
        for url in link_list:
            f1.writelines('https://download.geoservice.dlr.de/WSF2019/files/' + url)
            f1.write('\n')
        f1.close()

    def __str__(self):
        return '包含土地利用覆盖数据产品：GLC1985-2020全球30m土地覆被数据集、清华大学glc2017全球30m土地覆被数据、GlobaLand30_2000、2010、2020年全球土地覆被数据'


def downDataset_GLC(api, datasetName):
    dataset = landcover()
    if not os.path.exists(dataset.dataset[datasetName][0][:-4] + 'getAllFileListBySdoId.json'):
        data_json = json.loads(requests.get(api).text)
        # save json
        with open(dataset.dataset[datasetName][0][:-4] + 'getAllFileListBySdoId.json', 'w') as f:
            f.write(json.dumps(data_json, ensure_ascii=False))
    else:
        with open(dataset.dataset[datasetName][0][:-4] + 'getAllFileListBySdoId.json', 'r') as f:
            data_json = json.loads(f.read())
    # save download url
    total_file = data_json['总文件数']
    count = 0
    f1 = open(dataset.dataset[datasetName][0][:-4] + 'downURL.txt', 'w')
    for data in data_json['文件信息列表']:
        id = data['id']
        filename = data['filename']
        downloadOneFile = f'https://data.casearth.cn/sdo/downloadOneFile?id={id}&username=1921134176@qq.com'
        f1.writelines(downloadOneFile)
        f1.write('\n')
        if not os.path.exists(dataset.dataset[datasetName][0] + filename):
            wget.download(downloadOneFile, out=dataset.dataset[datasetName][0] + filename)
        count += 1
        if count % 5 == 0:
            print('thread %s is running...' % multiprocessing.current_process().name)
            print(f'{datasetName}已完成：{count}/{total_file}')
    f1.close()
    print(f'{datasetName}已完成：{count / total_file}')
    print('thread %s is over!!！' % os.getpid())


def downDataset_WSF2015_2019(datasetName):
    dataset = landcover()
    count = 0
    with open(dataset.dataset[datasetName][0][:-4] + 'downURL.txt', 'r') as f1:
        urls = f1.readlines()
    for url in urls:
        if not os.path.exists(dataset.dataset[datasetName][0] + url.split('/')[-1].strip()):
            wget.download(url.strip(), out=dataset.dataset[datasetName][0] + url.split('/')[-1].strip())
        count += 1
        if count % 5 == 0:
            print('thread %s is running...' % multiprocessing.current_process().name)
            print(f'{datasetName}已完成：{count}/{len(urls)}')
    print(f'{datasetName}已完成：{count}/{len(urls)}')
    print('thread %s is over!!！' % os.getpid())


if __name__ == "__main__":
    # glc = landcover()
    # glc.download_GLC2017_Tinghua()
    # glc.download_GLC_ChineseAcademyOfSciences()
    # print(landcover())
    # glc.download_WSF2019()

    # 并行下载wsf数据
    pool = multiprocessing.Pool(2)
    pool.apply_async(downDataset_WSF2015_2019, args=('WSF2015',))
    pool.apply_async(downDataset_WSF2015_2019, args=('WSF2019',))
    pool.close()
    pool.join()
