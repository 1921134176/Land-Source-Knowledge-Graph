#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
@author: ChengXin
@contact:1921134176@qq.com
@version: 1.0.0
@license: Apache Licence
@file: data.py
@time: 2021/10/29 13:44
"""
import requests
import json
import os
from conutrykg import convert
import pandas as pd
import geopandas
from shapely.geometry import Point, Polygon, shape


class District:
    def __init__(self):
        self.key_tianditu = '52f00b8da23736f0b4788191f29234d0'
        self.key_gaode = 'e3dce9a452e4d566d6f6b0dc3741273c'
        self.country = {}
        self.province = {}
        self.city = {}
        self.district = {}
        self.shapeFileDir = ''
        self.district_level()
        self.shapeFileDir = self.get_polyline_center()

    def district_level(self, dataFile='..\\data\\ChineseDistricts\\'):
        chineseDistricts = os.listdir(dataFile)
        if len(chineseDistricts) >= 4:
            with open('..\\data\\ChineseDistricts\\country.txt', 'r') as f:
                self.country = json.loads(f.read())
            with open('..\\data\\ChineseDistricts\\province.txt', 'r') as f:
                self.province = json.loads(f.read())
            with open('..\\data\\ChineseDistricts\\city.txt', 'r') as f:
                self.city = json.loads(f.read())
            with open('..\\data\\ChineseDistricts\\district.txt', 'r') as f:
                self.district = json.loads(f.read())
        else:
            print(("行政区划数据获取中..."))
            # 获取国>省>市>区
            data = self.get_district_from_gaode('中国')
            self.country['-'.join([data['districts'][0]['name'], data['districts'][0]['level']])] = [[province["name"], province["level"]] for province in data['districts'][0]['districts']]
            for province in data['districts'][0]['districts']:
                self.province['-'.join([province["name"], province['level']])] = [[city["name"], city["citycode"], city["adcode"], city["level"]] for city in province['districts']]
            for province in data['districts'][0]['districts']:
                for city in province['districts']:
                    self.city['-'.join([city["name"], city["citycode"], city["adcode"], city['level']])] = [[district["name"], district["citycode"], district["adcode"], district["level"]] for district in city['districts']]
            # 区和街道有重名
            for _, districts in self.city.items():
                for district in districts:
                    if district[3] == 'district':
                        self.district['-'.join(district)] = [[street["name"], street["citycode"], street["adcode"], street['level']] for
                                                            street in self.get_district_from_gaode(district[2])['districts'][0]['districts']]
            with open('..\\data\\ChineseDistricts\\country.txt', 'w') as f:
                f.write(json.dumps(self.country, ensure_ascii=False))
            with open('..\\data\\ChineseDistricts\\province.txt', 'w') as f:
                f.write(json.dumps(self.province, ensure_ascii=False))
            with open('..\\data\\ChineseDistricts\\city.txt', 'w') as f:
                f.write(json.dumps(self.city, ensure_ascii=False))
            with open('..\\data\\ChineseDistricts\\district.txt', 'w') as f:
                f.write(json.dumps(self.district, ensure_ascii=False))

    def get_polyline_center(self, dataFile='..\\data\\ChineseDistrictShape\\'):
        """
        获取矢量边界，并保存
        :return:
        """
        centerDict = {}
        polylineGDF = pd.DataFrame({'name': [], 'geometry': []})
        # centersDF = pd.DataFrame({'name': [], 'geometry': []})
        if os.path.exists(os.path.join(dataFile, 'center.txt')):
            with open(os.path.join(dataFile, 'center.txt'), 'r') as f:
                centerDict = json.loads(f.read())
        # if os.path.exists(os.path.join(dataFile, 'center_point')):
        #     centersDF = geopandas.read_file(os.path.join(dataFile, 'center_point'), encoding='utf-8')
        #     centersDF = pd.DataFrame(centersDF)
        if os.path.exists(os.path.join(dataFile, '中国行政区划')):
            polylineGDF = geopandas.read_file(os.path.join(dataFile, '中国行政区划'), encoding='utf-8')
            polylineGDF = pd.DataFrame(polylineGDF)
        # 判断数据是否已经存在并且完整
        if not set(list(self.country.keys()) + list(self.province.keys()) + list(self.city.keys()) +
                   list(self.district.keys())).issubset(set(os.listdir(dataFile))):
            print(("矢量数据获取中..."))
            nameList = list(self.country.keys()) + list(self.province.keys()) + list(self.city.keys()) + list(
                self.district.keys())
            nameList = list(set(nameList) - set(os.listdir(dataFile)))
            for name in nameList:
                if 'district' in name:
                    nameInfo = self.get_district_from_gaode(name.split('-')[2])
                else:
                    nameInfo = self.get_district_from_gaode(name.split('-')[0])
                if nameInfo['count'] != '1':
                    for i in nameInfo['districts']:
                        if i['name'] == name.split('-')[0]:
                            temp = i
                    nameInfo['districts'] = [temp]
                    # print(nameInfo['districts'][0]['name'], '-', nameInfo['districts'][0]['citycode'], '-', nameInfo['districts'][0]['adcode'], '-', nameInfo['districts'][0]['level'])
                    # continue
                polyline = nameInfo['districts'][0]['polyline']
                center = nameInfo['districts'][0]['center']
                if nameInfo['districts'][0]['districts'] != [] and nameInfo['districts'][0]['districts'][0]['level'] == 'street':
                    for street in nameInfo['districts'][0]['districts']:
                        centerStreet = street['center']
                        centerStreet_wgs84 = convert.gcj02_to_wgs84(*[float(temp) for temp in centerStreet.split(',')])
                        centerDict['-'.join([street['name'], street['citycode'], street['adcode'], street['level']])] = centerStreet_wgs84
                # 将火星坐标转化为WGS-84坐标
                center_wgs84 = convert.gcj02_to_wgs84(*[float(temp) for temp in center.split(',')])
                centerDict[name] = center_wgs84
                polylineDataFrame = pd.DataFrame({'name': [], 'geometry': []})
                for subPolyline in polyline.split('|'):
                    polylineList = [list(map(lambda x: float(x), temp.split(','))) for temp in subPolyline.split(';')]
                    polyline_wgs84 = list(map(lambda x: convert.gcj02_to_wgs84(*x), polylineList))
                    polylineDataFrame = polylineDataFrame.append(pd.DataFrame({'name': [name], 'geometry': [Polygon(polyline_wgs84)]}))
                polylineGeoDataFrame = geopandas.GeoDataFrame(polylineDataFrame)
                polylineGeoDataFrame.to_file(os.path.join(dataFile, name), encoding='utf-8')
                polylineGeoDataFrame.to_file(os.path.join(dataFile, name+'.json'), driver='GeoJSON', encoding='utf-8')
                polylineGDF = polylineGDF.append(polylineDataFrame)
            polylineGDF = geopandas.GeoDataFrame(polylineGDF)
            polylineGDF.to_file(os.path.join(dataFile, '中国行政区划'), encoding='utf-8')
            polylineGDF.to_file(os.path.join(dataFile, '中国行政区划' + '.json'), driver='GeoJSON', encoding='utf-8')
            with open(os.path.join(dataFile, 'center.txt'), 'w') as f:
                f.write(json.dumps(centerDict, ensure_ascii=False))
            centerList = list(centerDict.items())
            centerName = [[item[0]] for item in centerList]
            centerDF = pd.DataFrame(centerName, columns=['name'])
            centerPoint = [Point(*item[1]) for item in centerList]
            centerDF["geometry"] = centerPoint
            centerGDF = geopandas.GeoDataFrame(centerDF)
            centerGDF.to_file(os.path.join(dataFile, 'center_point'), encoding='utf-8')
            centerGDF.to_file(os.path.join(dataFile, 'center_point' + '.json'), driver='GeoJSON', encoding='utf-8')
            print("数据已加载！")
            return dataFile
        else:
            print("数据已加载！")
            return dataFile

    def get_district_from_gaode(self, name, key=None):
        """
        调用高德接口通过行政区划名称来获取该行政区划的基本学习及其下级行政区划信息
        :param name:
        :param key:
        :return:
        """
        if key is None:
            key = self.key_gaode
        else:
            self.key_gaode = key
        url = "https://restapi.amap.com/v3/config/district?key=" + key + "&keywords=" + name + "&subdistrict=3&page=1&extensions=all&output=json"
        payload = {}
        headers = {}
        response = requests.request("GET", url, headers=headers, data=payload)
        data = json.loads(response.text)
        return data

    def get_district_from_tianditu(self, name, key=None):
        """
        调用天地图接口通过行政区划名称来获取该行政区划的基本学习及其下级行政区划信息
        :param name:
        :param key:
        :return:
        """
        if key is None:
            key = self.key_tianditu
        else:
            self.key_tianditu = key
        url = "http://api.tianditu.gov.cn/administrative?postStr={\"searchWord\":\"" + name + "\",\"searchType\":\"1\"," \
                                                                                              "\"needSubInfo\":\"true\",\"needAll\":\"true\",\"needPolygon\":\"true\",\"needPre\":\"false\"}" \
                                                                                              "&tk=" + key
        payload = {}
        headers = {'Cookie': 'HWWAFSESID=0b8041570dc585a3e53; HWWAFSESTIME=1635484776412'}
        response = requests.request("GET", url, headers=headers, data=payload)
        data = json.loads(response.text)
        return data


if __name__ == "__main__":
    district = District()
    data = district.get_district_from_gaode('中国')
    print(data['status'])