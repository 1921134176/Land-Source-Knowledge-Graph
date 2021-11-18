#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
@author: ChengXin
@contact:1921134176@qq.com
@version: 1.0.0
@license: Apache Licence
@file: district_graph.py
@time: 2021/11/9 15:44
"""
from py2neo import Graph, Node, Relationship
from conutrykg import districts
import os
import pandas as pd
import geopandas
from shapely.geometry import Point, Polygon, shape


class DistrictGraph:
    def __init__(self, port=7474, name='neo4j', password='201314'):
        # 数据库连接
        self.neo4j_name = name
        self.neo4j_password = password
        self.graph = Graph(f"http://localhost:{port}", auth=(self.neo4j_name, self.neo4j_password))
        # 初始化行政单位数据
        self.districts = districts.District()

    def create_districts_graph(self):
        """
        根据高德行政区划api爬取的中国行政区划，创建知识图谱
        :return:
        """
        # 在节点的alias别名属性上创建唯一值约束
        if not self.graph.schema.node_labels:
            self.graph.schema.create_uniqueness_constraint('country', 'alias')
            self.graph.schema.create_uniqueness_constraint('province', 'alias')
            self.graph.schema.create_uniqueness_constraint('city', 'alias')
            self.graph.schema.create_uniqueness_constraint('district', 'alias')
            self.graph.schema.create_uniqueness_constraint('street', 'alias')
        # 创建country字典节点
        countryList = list(self.districts.country.items())
        for country, provinceList in countryList:
            # 由于地名会存在同名情况，因此添加了一个alias别名属性来添加索引和约束
            country_node = Node(country.split('-')[-1], name=country.split('-')[0], alias=country)
            self.graph.merge(country_node, country.split('-')[-1], 'alias')
            for province in provinceList:
                province_node = Node(province[1], name=province[0], alias='-'.join(province))
                relationship = Relationship(country_node, province[1], province_node)
                self.graph.merge(relationship, province[1], 'alias')
        total_province = sum(len(j) for i, j in list(district_graph.districts.country.items()))
        print(f'导入province节点：{total_province}')
        # 创建province字典节点
        provinceList = list(self.districts.province.items())
        for province, cityList in provinceList:
            if cityList == []:
                continue
            province_node = self.graph.nodes.match(province.split('-')[-1], alias=province).first()
            for city in cityList:
                city_node = Node(city[-1], name=city[0], alias='-'.join(city))
                relationship = Relationship(province_node, city[-1], city_node)
                self.graph.merge(relationship, city[-1], 'alias')
        total_city = sum(len(j) for i, j in list(district_graph.districts.province.items()))
        print(f'导入city节点：{total_city}')
        # 创建city字典节点
        cityList = list(self.districts.city.items())
        for city, districtList in cityList:
            if districtList == []:
                continue
            city_node = self.graph.nodes.match(city.split('-')[-1], alias=city).first()
            for district in districtList:
                district_node = Node(district[-1], name=district[0], alias='-'.join(district))
                relationship = Relationship(city_node, district[-1], district_node)
                self.graph.merge(relationship, district[-1], 'alias')
        total_district = sum(len(j) for i, j in list(district_graph.districts.city.items()))
        print(f'导入district节点：{total_district}')
        # 创建district字典节点
        districtList = list(self.districts.district.items())
        for district, streetList in districtList:
            if streetList == []:
                continue
            district_node = self.graph.nodes.match(district.split('-')[-1], alias=district).first()
            for street in streetList:
                street_node = Node(street[-1], name=street[0], alias='-'.join(street))
                relationship = Relationship(district_node, street[-1], street_node)
                self.graph.merge(relationship, street[-1], 'alias')
        total_street = sum(len(j) for i, j in list(district_graph.districts.district.items()))
        print(f'导入street节点：{total_street}')
        print(f"导入总行政区划节点：{total_street + total_district + total_city + total_province + 1}")

    def add_spatial_point(self):
        """
        将行政区划的中心坐标点加入知识图谱节点
        :return:
        """
        if os.path.exists(os.path.join(self.districts.shapeFileDir, 'center_point')):
            center_point = geopandas.read_file(os.path.join(self.districts.shapeFileDir, 'center_point'),
                                               encoding='utf-8')
            centerPointDF = pd.DataFrame(center_point)
            for index, row in centerPointDF.iterrows():
                alias = row['name']
                point = row['geometry']
                latitude = point.y
                longitude = point.x
                node = self.graph.nodes.match(alias=alias).all()
                if len(node) == 1:
                    self.graph.run(f"match(n) where n.alias = '{alias}' set n.center=point({{latitude: {latitude}, longitude: {longitude}}})")
                else:
                    print(node, '--未处理')
                if (index+1) % 200 == 0:
                    print(f'已处理{index+1}/{len(self.graph)+1}')
        else:
            print('数据不存在！！！')


    def create_landcover_graph(self):
        """
        根据不同数据集按照行政区划统计的数据添加进中国行政区划图谱
        :return:
        """
        pass

    def __str__(self):
        return f'DataBase {self.neo4j_name}:{len(self.graph.nodes)} nodes, {len(self.graph.relationships)} relationships'


if __name__ == "__main__":
    district_graph = DistrictGraph(11005)
    # district_graph.graph.delete_all()
    # district_graph.create_districts_graph()
    district_graph.add_spatial_point()
    # district_graph.create_landcover_graph()
    print('end')
