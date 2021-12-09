#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
glc2020与globaland30_2020一致性比较
@author: ChengXin
@contact:1921134176@qq.com
@version: 1.0.0
@license: Apache Licence
@file: glc2020_compare_globaland30.py
@time: 2021/12/7 12:39
"""
from pandas import Series
import pandas as pd
import os
import glob
from osgeo import gdal
from py2neo import Graph, Node, Relationship


def graph_connect(port=7474, name='neo4j', password='neo4j'):
    graph = Graph(f"http://localhost:{port}", auth=(name, password))
    return graph


def consistency_raster(raster1FilePath, raster2FilePath):
    raster1 = gdal.Open(raster1FilePath).ReadAsArray()
    raster2 = gdal.Open(raster2FilePath).ReadAsArray()
    row = min(raster1.shape[0], raster2.shape[0])
    col = min(raster1.shape[1], raster2.shape[1])
    raster1 = raster1[:row, :col]
    raster2 = raster2[:row, :col]
    # 空间一致性N
    N = sum(sum(raster1 == raster2)) / sum(sum(raster2 != 0)) * 100
    # 空间一致性Mi
    classMap = {10: 'Cultivated Land', 20: 'Forest', 30: 'Grassland', 40: 'Shrubland', 50: 'Wetland', 60: 'Water',
                80: 'Impervious surface', 90: 'Bareland', 100: 'Snow/Ice'}
    mDict = {}
    same_mask = (raster1 == raster2)
    for i in [10, 20, 30, 40, 50, 60, 80, 90, 100]:
        pi2 = sum(sum((raster1 == i) * same_mask)) * 2
        xi_yi = (sum(sum(raster1 == i)) + sum(sum(raster2 == i)))
        if pi2 == 0 and xi_yi == 0:
            M = 100
        else:
            M = pi2 / xi_yi * 100
        mDict[classMap[i]] = M
    # 面积一致性
    raster1_area_statistics = []
    raster2_area_statistics = []
    PDDict = {}
    for i in [10, 20, 30, 40, 50, 60, 80, 90, 100]:
        raster1_area_statistics.append(sum(sum(raster1 == i)))
        raster2_area_statistics.append(sum(sum(raster2 == i)))
        if raster1_area_statistics[-1] == 0 and raster2_area_statistics[-1] == 0:
            PD = 0
        else:
            PD = abs(raster1_area_statistics[-1] - raster2_area_statistics[-1]) / (raster1_area_statistics[-1] + raster2_area_statistics[-1]) *100
        PDDict[classMap[i]] = PD
    raster1_area_statistics = Series(raster1_area_statistics)
    raster2_area_statistics = Series(raster2_area_statistics)
    R = raster1_area_statistics.corr(raster2_area_statistics) * 100
    return N, mDict, R, PDDict


def consistency_raster_dataset(rasterDirPath1, rasterDirPath2):
    if not os.path.exists(os.path.join(rasterDirPath1, 'statistics')):
        os.mkdir(os.path.join(rasterDirPath1, 'statistics'))
    raster1FilePathList = glob.glob(os.path.join(rasterDirPath1, '*.tif'))
    # 数据存储在二维表格中
    table = pd.DataFrame()
    count = 0
    for raster1FilePath in raster1FilePathList:
        dataDict = {}
        rasterName = os.path.basename(raster1FilePath).split('.')[0]
        raster2FilePath = os.path.join(rasterDirPath2, os.path.split(raster1FilePath)[1])
        N, mDict, R, PDDict = consistency_raster(raster1FilePath, raster2FilePath)
        dataDict['name'] = rasterName
        dataDict['N'] = N
        dataDict['R'] = R
        for k, v in mDict.items():
            dataDict[k + '-M'] = v
        for k, v in PDDict.items():
            dataDict[k + '-PD'] = v
        temp = pd.DataFrame(dataDict, index=[dataDict['name']])
        # 将数据写进dataframe
        table = table.append(temp, verify_integrity=True)
        # 添加进知识图谱
        graph = graph_connect(port=11005, name='neo4j', password='201314')
        node_N = Node('proxy', name='总体一致性系数-N', data=N)
        node_R = Node('proxy', name='皮尔森相关系数-R', data=R)
        max_M = [(k, v) for k, v in sorted(mDict.items(), key=lambda item: item[1]) if v != 100][-1]
        min_PD = [(k, v) for k, v in sorted(PDDict.items(), key=lambda item: item[1], reverse=True) if v != 0][-1]
        node_Mi = Node('proxy', name='不同地类的一致性系数-Mi', data=max_M[1], category=max_M[0], filterType='max')
        node_PD = Node('proxy', name='不同地类百分比不一致性-PD', data=min_PD[1], category=min_PD[0], filterType='min')
        data = graph.run(f"match p=({{alias:'{rasterName}'}})-[r:Landcover_Dataset]->(n)"
                         f" where r.name='glc_2020_30m' or r.name='GlobaLand30_2020' "
                         f"return n, r.name as dataName").data()
        if data[0]['dataName'] == 'glc_2020_30m':
            node_glc2020 = graph.nodes.match('statistics', data=data[0]['n']['data']).all()[-1]
            node_globaland30 = graph.nodes.match('statistics', data=data[1]['n']['data']).all()[-1]
        else:
            node_glc2020 = graph.nodes.match('statistics', data=data[0]['n']['data']).all()[-1]
            node_globaland30 = graph.nodes.match('statistics', data=data[1]['n']['data']).all()[-1]
        node_glc2020_node_N = Relationship(node_glc2020, 'compare_with', node_N, compare_dataset='GlobaLand30_2020',
                                           proxy='总体一致性系数-N')
        node_glc2020_node_R = Relationship(node_glc2020, 'compare_with', node_R, compare_dataset='GlobaLand30_2020',
                                           proxy='皮尔森相关系数-R')
        node_glc2020_node_Mi = Relationship(node_glc2020, 'compare_with', node_Mi, compare_dataset='GlobaLand30_2020',
                                            proxy='不同地类的一致性系数-Mi', filterType='max')
        node_glc2020_node_PD = Relationship(node_glc2020, 'compare_with', node_PD, compare_dataset='GlobaLand30_2020',
                                            proxy='不同地类百分比不一致性-PD', filterType='min')
        node_globaland30_node_N = Relationship(node_globaland30, 'compare_with', node_N, compare_dataset='glc_2020_30m',
                                               proxy='总体一致性系数-N')
        node_globaland30_node_R = Relationship(node_globaland30, 'compare_with', node_R, compare_dataset='glc_2020_30m',
                                               proxy='皮尔森相关系数-R')
        node_globaland30_node_Mi = Relationship(node_globaland30, 'compare_with', node_Mi, compare_dataset='glc_2020_30m',
                                                proxy='不同地类的一致性系数-Mi', filterType='max')
        node_globaland30_node_PD = Relationship(node_globaland30, 'compare_with', node_PD,
                                                compare_dataset='glc_2020_30m',
                                                proxy='不同地类百分比不一致性-PD', filterType='min')
        graph.create(node_glc2020_node_N)
        graph.create(node_glc2020_node_R)
        graph.create(node_glc2020_node_Mi)
        graph.create(node_glc2020_node_PD)
        graph.create(node_globaland30_node_N)
        graph.create(node_globaland30_node_R)
        graph.create(node_globaland30_node_Mi)
        graph.create(node_globaland30_node_PD)
        for k, v in mDict.items():
            if v != 100:
                r = Relationship(node_Mi, 'category', Node('proxy', name='不同地类的一致性系数-Mi', data=v, category=k), name=k)
                graph.create(r)
        for k, v in PDDict.items():
            if v != 0:
                r = Relationship(node_PD, 'category', Node('proxy', name='不同地类百分比不一致性-PD', data=v, category=k), name=k)
                graph.create(r)
        count += 1
        if count % 20 == 0:
            print(f'已完成{count}/{len(raster1FilePathList)}')
    # table.to_csv(os.path.join(rasterDirPath1, 'statistics', 'districts_statistics.csv'))
    print(f'已完成{count}/{len(raster1FilePathList)}')



if __name__ == "__main__":
    glc2020DirPath = '..\\data\\ChineseLandcover\\GLC2020_resample_reclasspy_base_GlobaLand30_2020'
    globaland30DirPath = '..\\data\\ChineseLandcover\\GlobaLand30_2020'
    consistency_raster_dataset(glc2020DirPath, globaland30DirPath)
    print('end')
