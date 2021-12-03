#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
根据行政区划获取国土资源的栅格文件
@author: ChengXin
@contact:1921134176@qq.com
@version: 1.0.0
@license: Apache Licence
@file: get_raster.py
@time: 2021/12/1 14:52
"""
from py2neo import Graph, Node, Relationship
import sys
import os
from shutil import copyfile
import json

port = 11005
name = 'neo4j'
password = '201314'
graph = Graph(f"http://localhost:{port}", auth=(name, password))
chineseLandCoverPath = 'G:\\投稿论文\\code\\data\\ChineseLandcover\\'
landCoverList = os.listdir(chineseLandCoverPath)


def getRasterFromDistrict(districtName=None, cityName=None, savePath='.\\'):
    """
    获取区级的行政区划土地覆盖数据
    可以输入区名或是输入市名，获取市级下所有区级土地覆盖数据
    :param districtNaame:
    :param cityName:
    :return:
    """
    if not districtName is None and cityName is None:
        if len(graph.nodes.match(name=districtName)) == 1:
            alias = graph.nodes.match(name=districtName).first()['alias']
        elif len(graph.nodes.match(name=districtName)) > 1:
            print('有多个同名区域，请使用区域唯一值别名搜索。')
            sys.exit()
        elif len(graph.nodes.match(name=districtName)) == 0 and len(graph.nodes.match(alias=districtName)) == 1:
            alias = districtName
        else:
            print('区域名称有误！')
        if not os.path.exists(os.path.join(savePath, alias)):
            os.mkdir(os.path.join(savePath, alias))
        for i in landCoverList:
            copyfile(os.path.join(chineseLandCoverPath, i, alias+'.tif'), os.path.join(savePath, alias, i+'_'+alias+'.tif'))
    elif districtName is None and not cityName is None:
        if len(graph.nodes.match(name=cityName)) == 1:
            aliasList = [i['n']['alias'] for i in graph.run(f"match p=(:city{{name:'{cityName}'}})-[:district]->(n) return n").data()]
        elif len(graph.nodes.match(name=cityName)) == 0 and len(graph.nodes.match(alias=cityName)) == 1:
            aliasList = [i['n']['alias'] for i in graph.run(f"match p=(:city{{name:'{cityName}'}})-[:district]->(n) return n").data()]
        else:
            print('市区域名称有误！')
        if not os.path.exists(os.path.join(savePath, cityName)):
            os.mkdir(os.path.join(savePath, cityName))
        for alias in aliasList:
            if not os.path.exists(os.path.join(savePath, cityName, alias)):
                os.mkdir(os.path.join(savePath, cityName, alias))
            for i in landCoverList:
                copyfile(os.path.join(chineseLandCoverPath, i, alias + '.tif'),
                         os.path.join(savePath, cityName, alias, i + '_' + alias + '.tif'))
    else:
        print('请输入合适的参数！')



def getRasterFromCityOrProvince(name, savePath='.\\'):
    """
    获取区级以上行政区划数据
    另需借助get_raster_arcgis.py进行栅格镶嵌
    :param name:
    :param savePath:
    :return:
    """
    nodeDict = {}
    if len(graph.nodes.match(name=name)) == 1:
        node = graph.nodes.match(name=name).first()
        aliasList = [i['n']['alias'] for i in graph.run(
            f"match p=(n1{{name:'{name}'}})-[*]->(n)-[:Landcover_Dataset{{name:'glc_1985_30m'}}]->(n3) return n").data()]
    elif len(graph.nodes.match(name=name)) == 0 and len(graph.nodes.match(alias=name)) == 1:
        node = graph.nodes.match(name=name).first()
        aliasList = [i['n']['alias'] for i in
                     graph.run(f"match p=(:city{{name:'{name}'}})-[:district]->(n) return n").data()]
    else:
        print('名称有误！')
    nodeDict[name] = aliasList
    if not os.path.exists(os.path.join(savePath, name+'_CityOrProvince')):
        os.mkdir(os.path.join(savePath, name+'_CityOrProvince'))
    if not os.path.exists(os.path.join(savePath, name + '_CityOrProvince', 'node.json')):
        with open(os.path.join(savePath, name+'_CityOrProvince', 'node.json'), 'w', encoding='utf-8') as f:
            f.write(json.dumps(nodeDict, ensure_ascii=False))
    # 节点计算
    datasetList = [i['r'] for i in
                   graph.run("match()-[r:Landcover_Dataset]->() return distinct(properties(r)) as r").data()]
    categoryList = [i['r'] for i in
                   graph.run("match()-[r:category]->() return distinct(properties(r)) as r").data()]
    for dataset in datasetList:
        statistics_node_area = graph.run(f"match p=(m)-[*]->()-[:Landcover_Dataset{{name:'{dataset['name']}'}}]->(n) where m.name='{name}' "
                  f"or m.alias='{name}'  return n").data()
        area = sum([i['n']['data'] for i in statistics_node_area])
        nodeArea = Node('statistics', area=area, data=area, unit='KM2')
        r_area = Relationship(node, 'Landcover_Dataset', nodeArea)
        r_area.update(dataset)
        graph.create(r_area)
        for category in categoryList:
            percent = graph.run(f"match p=(m)-[*]->()-[:Landcover_Dataset{{name:'{dataset['name']}'}}]->(n)-"
                                f"[:category{{name:'{category['name']}'}}]->(n2) where m.name='{name}' or m.alias='{name}'"
                                f"  return n2").data()
            categoryArea = sum([i['n2']['area'] for i in percent])
            if categoryArea != 0:
                categoryNode = Node('statistics', area=categoryArea,
                                    data=categoryArea/area*100,
                                    percentage=categoryArea/area*100,
                                    unit='KM2')
                r_category = Relationship(nodeArea, 'category', categoryNode)
                r_category.update(category)
                graph.create(r_category)






if __name__ == "__main__":
    # getRasterFromDistrict(districtName='武昌区', savePath='G:\\投稿论文\\paper_content\\pic\\')
    # getRasterFromDistrict(cityName='武汉市', savePath='G:\\投稿论文\\paper_content\\pic\\')
    # getRasterFromCityOrProvince(name='武汉市', savePath='G:\\投稿论文\\paper_content\\pic\\')
    getRasterFromCityOrProvince(name='长江三角洲地区', savePath='G:\\投稿论文\\paper_content\\pic\\')
    print('end!!!')
