#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
@author: ChengXin
@contact:1921134176@qq.com
@version: 1.0.0
@license: Apache Licence
@file: rasterclip.py
@time: 2021/11/4 22:05
"""
from osgeo import gdal
import os
import shapefile
from conutrykg import gdal_merge, districts, landcover
from collections import Counter
import pandas as pd
import multiprocessing


def clipRaster(rasterName, shpPath, savePath, project='wgs84'):
    """
    矢量裁剪栅格，矢量和栅格需要保持相同的空间参考
    :param rasterName:
    :param shpPath:
    :param savePath:
    :param project: 选择不同投影的矢量文件
    :return:
    """
    # 要裁剪的原图
    input_raster = gdal.Open(rasterName)
    if project == 'wgs84':
        name = shpPath.split('\\')[-2]
        input_shape = shpPath + name + '.shp'
        r = shapefile.Reader(input_shape)
        output_raster = savePath + name + '.tif'
        ds = gdal.Warp(output_raster,
                       input_raster,
                       format='GTiff',
                       outputBounds=r.bbox,
                       cutlineDSName=input_shape,
                       dstNodata=0)
    elif project == 'utm':
        name = shpPath.split('\\')[-2]
        input_shape = shpPath + name + '_ProjectUTM.shp'
        r = shapefile.Reader(input_shape)
        output_raster = savePath + name + '.tif'
        ds = gdal.Warp(output_raster,
                       input_raster,
                       format='GTiff',
                       outputBounds=r.bbox,
                       cutlineDSName=input_shape,
                       dstNodata=0)
    ds = None


def shapeBound(shaPath):
    """
    返回矢量边界的最大范围——左、下、右、上
    :param shaPath:
    :return:
    """
    files = os.listdir(shaPath)
    for f in files:  # 循环读取路径下的文件并筛选输出
        if os.path.splitext(f)[1] == ".shp":
            input_shape = shaPath + f
            r = shapefile.Reader(input_shape)
            # 左、下、右、上
            bound = list(r.bbox)
            return bound


def glc2020Raster(bound, datasetPath):
    """
    利用边界获取裁剪所需要的影像文件
    :param bound:
    :param datasetPath:
    :return:
    """
    rasterList = []
    span = 5
    # fileNamePrefix = datasetPath + 'GLC_FCS30_2020_'  # GLC_FCS30_2020_E0N10.tif
    year = datasetPath.split('\\')[3][-4:]
    fileNamePrefix = datasetPath + f'GLC_FCS30_{year}_'  # GLC_FCS30_2020_E0N10.tif
    b_left, b_down, b_right, b_up = bound
    index_left = int(b_left // span * span)
    index_down = int(b_down // span * span)
    index_right = int((b_right // span + int(b_right % span > 0)) * span)
    index_up = int((b_up // span + int(b_up % span > 0)) * span)
    for i in range(index_left, index_right, span):
        for j in range(index_down, index_up, span):
            fileName = fileNamePrefix + f"E{i}N{j + 5}.tif"
            rasterList.append(fileName)
    return rasterList, [index_left, index_down, index_right, index_up]


def glc2017Raster(bound, datasetPath):
    """
        利用边界获取裁剪所需要的影像文件
        :param bound:
        :param datasetPath:
        :return:
        """
    rasterList = []
    span = 2
    fileNamePrefix = datasetPath + 'fromglc10v01_'  # fromglc10v01_0_8.tif
    b_left, b_down, b_right, b_up = bound
    index_left = int(b_left // span * span)
    index_down = int(b_down // span * span)
    index_right = int((b_right // span + int(b_right % span > 0)) * span)
    index_up = int((b_up // span + int(b_up % span > 0)) * span)
    for i in range(index_left, index_right, span):
        for j in range(index_down, index_up, span):
            fileName = fileNamePrefix + f"{j}_{i}.tif"
            rasterList.append(fileName)
    return rasterList, [index_left, index_down, index_right, index_up]


def GlobaLand2020Raster(bound, datasetPath, project='wgs84'):
    """
    利用边界获取裁剪所需要的影像文件
    :param bound:
    :param datasetPath:
    :return:
    """
    rasterList = []
    b_left, b_down, b_right, b_up = bound
    # 左边所在带号
    index_left = int(b_left//6 + 31)
    # 右边所在带号
    index_right = int(b_right//6 + 31)
    span = 5
    index_down = int(b_down // span * span)
    index_up = int((b_up // span + int(b_up % span > 0)) * span)
    year = datasetPath.split('\\')[3][-4:]
    if b_down <= 60 and b_up <= 60:
        for i in range(index_left, index_right+1):
            for j in range(index_down, index_up, span):
                if j == 0:
                    if project == 'wgs84':
                        fileName = datasetPath + f'N{i}_00_{year}LC030\\' + f'n{i}_00_{year}lc030_reproject_wgs84.tif'
                    elif project == 'utm':
                        fileName = datasetPath + f'N{i}_00_{year}LC030\\' + f'n{i}_00_{year}lc030.tif'
                elif j == 5:
                    if project == 'wgs84':
                        fileName = datasetPath + f'N{i}_05_{year}LC030\\' + f'n{i}_05_{year}lc030_reproject_wgs84.tif'
                    elif project == 'utm':
                        fileName = datasetPath + f'N{i}_05_{year}LC030\\' + f'n{i}_05_{year}lc030.tif'
                else:
                    if project == 'wgs84':
                        fileName = datasetPath + f'N{i}_{j}_{year}LC030\\' + f'n{i}_{j}_{year}lc030_reproject_wgs84.tif'
                    elif project == 'utm':
                        fileName = datasetPath + f'N{i}_{j}_{year}LC030\\' + f'n{i}_{j}_{year}lc030.tif'
                if os.path.exists(fileName):
                    rasterList.append(fileName)
    elif b_down >= 60 and b_up >= 60:
        if index_left % 2 == 0:
            index_left -= 1
        if index_right % 2 == 0:
            index_right -= 1
        for i in range(index_left, index_right+2, 2):
            for j in range(index_down, index_up, span):
                if project == 'wgs84':
                    fileName = datasetPath + f'N{i}_{j}_{year}LC030\\' + f'n{i}_{j}_{year}lc030_reproject_wgs84.tif'
                elif project == 'utm':
                    fileName = datasetPath + f'N{i}_{j}_{year}LC030\\' + f'n{i}_{j}_{year}lc030.tif'
                if os.path.exists(fileName):
                    rasterList.append(fileName)
    elif b_down <= 60 and b_up >= 60:
        if index_left % 2 == 0:
            index_left -= 1
        if index_right % 2 == 0:
            index_right -= 1
        for i in range(index_left, index_right+2, 2):
            for j in range(60, index_up, span):
                if project == 'wgs84':
                    fileName = datasetPath + f'N{i}_{j}_{year}LC030\\' + f'n{i}_{j}_{year}lc030_reproject_wgs84.tif'
                elif project == 'utm':
                    fileName = datasetPath + f'N{i}_{j}_{year}LC030\\' + f'n{i}_{j}_{year}lc030.tif'
                if os.path.exists(fileName):
                    rasterList.append(fileName)
        for i in range(index_left, index_right+2):
            for j in range(index_down, 60, span):
                if project == 'wgs84':
                    fileName = datasetPath + f'N{i}_{j}_{year}LC030\\' + f'n{i}_{j}_{year}lc030_reproject_wgs84.tif'
                elif project == 'utm':
                    fileName = datasetPath + f'N{i}_{j}_{year}LC030\\' + f'n{i}_{j}_{year}lc030.tif'
                if os.path.exists(fileName):
                    rasterList.append(fileName)
    return rasterList, [index_left, index_down, index_right, index_up]


def rasterMosaic(rasterList, outputPath, outputName):
    """
    栅格数据镶嵌
    :param rasterList:
    :param outputPath:
    :param outputName:
    :return:
    """
    arg = ['gdal_merge',
           '-init', '0',
           '-o']
    arg.append(outputPath + outputName)
    arg.extend(rasterList)
    gdal_merge.main(arg)


def clipDatasatRasterFromShp(datasetPath, shapePath, outputPath, glc, project='wgs84'):
    """
    从产品数据集中自动拼接矢量裁剪所需要的影像并裁剪
    :param datasetPath:
    :param ShapePath:
    :param outputPath:
    :param glc:选择对应数据集处理函数
    :param project: 选择不同投影的矢量文件
    :return:
    """
    if not os.path.exists(outputPath):
        os.mkdir(outputPath)
    # 获取矢量最大外接矩形的边界范围
    bound = shapeBound(shapePath)
    # 根据范围在数据产品中筛选出裁剪所需要的影像
    rasterList, rasterBound = glc(bound, datasetPath, project)
    if len(rasterList) > 1:
        if os.path.exists(outputPath + '-'.join([str(i) for i in rasterBound]) + '.tif'):
            # 裁剪
            clipRaster(outputPath + '-'.join([str(i) for i in rasterBound]) + '.tif', shapePath, outputPath, project)
        else:
            # 将影像进行拼接
            rasterMosaic(rasterList, outputPath, '-'.join([str(i) for i in rasterBound]) + '.tif')
            # 裁剪
            clipRaster(outputPath + '-'.join([str(i) for i in rasterBound]) + '.tif', shapePath, outputPath, project)
    else:
        # 裁剪
        clipRaster(rasterList[0], shapePath, outputPath, project)
    # os.remove(outputPath + 'out.tif')


def baseDistricts():
    """
    获取包含边界数据的最小级别的行政区划：区级以及部分市级（下一级直接为街道）行政区划
    :return:
    """
    shapeNameList = []
    ChineseDistrict = districts.District()
    citys = ChineseDistrict.city
    for city, district in list(citys.items()):
        if district == []:
            shapeNameList.append(city)
        elif district[0][3] == 'district':
            shapeNameList.extend(['-'.join(i) for i in district])
        else:
            shapeNameList.append(city)
    shapeFilePath = list(
        map(lambda x: x + '\\', list(map(lambda x: os.path.join(ChineseDistrict.shapeFileDir, x), shapeNameList))))
    shapeFilePath.append(ChineseDistrict.shapeFileDir + '台湾省-province\\')
    return shapeFilePath


def ChineseDistrictsLandcover(rasterDataName='all', project='wgs84'):
    """
    裁剪出中国区级以及部分市级（下一级直接为街道）行政区划的土地覆盖数据
    :param rasterDataName: 裁剪的数据集名称，默认为landcover中的所有数据集。
    可选：'glc_2020_30m'、
    :param glc:选择对应数据集处理函数
    :param project: 选择不同投影的矢量文件
    :return:
    """
    # 获取所有需要裁剪的矢量文件路径
    shapeFilePath = baseDistricts()
    # 获取土地覆盖数据集路径
    landcoverDataset = landcover.landcover()
    if rasterDataName == 'all':
        for datasetName, path in list(landcoverDataset.dataset.items()):
            print('土地覆盖产品数据源：', datasetName)
            count = 0
            datasetPath = path[0]
            outputPath = path[1]
            # 裁剪
            for shapePath in shapeFilePath:
                if os.path.exists(outputPath + shapePath.split('\\')[-2] + '.tif'):
                    count += 1
                    if count % 200 == 0:
                        print(f'thread {multiprocessing.current_process().name} is running...' + f'已完成：{count}/{len(shapeFilePath)}')
                    continue
                clipDatasatRasterFromShp(datasetPath, shapePath, outputPath, landcoverDataset.datasetProcessFunction[datasetName])
                count += 1
                if count % 200 == 0:
                    print(f'已完成：{count}/{len(shapeFilePath)}')
            print(f'已完成：{count}/{len(shapeFilePath)}')
    else:
        path = landcoverDataset.dataset[rasterDataName]
        print('土地覆盖产品数据源：', rasterDataName)
        count = 0
        datasetPath = path[0]
        outputPath = path[1]
        # 裁剪
        for shapePath in shapeFilePath:
            if os.path.exists(outputPath + shapePath.split('\\')[-2] + '.tif'):
                count += 1
                if count % 200 == 0:
                    print(f'thread {multiprocessing.current_process().name} is running...' + f'已完成：{count}/{len(shapeFilePath)}')
                continue
            clipDatasatRasterFromShp(datasetPath, shapePath, outputPath, landcoverDataset.datasetProcessFunction[rasterDataName], project)
            count += 1
            if count % 200 == 0:
                print(f'thread {multiprocessing.current_process().name} is running...' + f'已完成：{count}/{len(shapeFilePath)}')
        print(f'thread {multiprocessing.current_process().name} is running...' + f'已完成：{count}/{len(shapeFilePath)}')


def ChineseDistrictsLandcoverStatistics(rasterDataName='all'):
    """
    对裁剪后的区块土地覆盖信息进行统计
    :param rasterDataName: 数据集名称，默认为landcover中的所有数据集。
    可选：'glc_2020_30m'、
    :return:
    """
    # 获取所有需要统计的行政区划名称
    shapeFilePath = baseDistricts()
    districtName = [i.split('\\')[-2] for i in shapeFilePath]
    # 获取土地覆盖数据集路径
    landcoverDataset = landcover.landcover()
    # 数据存储在二维表格中
    table = pd.DataFrame()
    if rasterDataName == 'all':
        for datasetName, path in list(landcoverDataset.dataset.items()):
            print('土地覆盖产品数据源：', datasetName)
            count = 0
            outputPath = path[1]
            # 判断文件夹是否存在，不存在则创建
            if not os.path.exists(outputPath + 'Statistics\\'):
                os.mkdir(outputPath + 'Statistics\\')
                # 遍历所有区划并统计
            for district in districtName:
                ds = gdal.Open(outputPath + district + '.tif')
                rasterArray = ds.ReadAsArray()
                statistics = Counter(rasterArray.flatten())
                allClassCount = dict(landcoverDataset.count[rasterDataName])
                for i in list(dict(statistics).keys()):
                    if i in list(allClassCount.keys()):
                        allClassCount[i] = statistics[i]
                pixel = sum(list(allClassCount.values()))
                # 面积(KM*2)
                area = pixel * pow((landcoverDataset.resolution[rasterDataName]) / 1000, 2)
                for key in list(allClassCount.keys()):
                    allClassCount[key] = allClassCount[key] / pixel
                    allClassCount[landcoverDataset.value_to_className[rasterDataName][key]] = allClassCount.pop(key)
                allClassCount['name'] = district
                allClassCount['area'] = area
                temp = pd.DataFrame(allClassCount, index=[allClassCount['name']])
                # 将数据写进dataframe
                table = table.append(temp, verify_integrity=True)
                ds = None
                count += 1
                if count % 50 == 0:
                    print(f'thread {multiprocessing.current_process().name} is running...' + f'已完成：{count}/{len(shapeFilePath)}')
                # 把name列调到第一列
            name = table['name']
            table.drop(labels=['name'], axis=1, inplace=True)
            table.insert(0, 'name', name)
            table.to_csv(outputPath + 'Statistics\\' + 'districts_statistics.csv')
            print(f'thread {multiprocessing.current_process().name} is running...' + f'已完成：{count}/{len(shapeFilePath)}')
    else:
        path = landcoverDataset.dataset[rasterDataName]
        print('土地覆盖产品数据源：', rasterDataName)
        count = 0
        outputPath = path[1]
        # 判断文件夹是否存在，不存在则创建
        if not os.path.exists(outputPath + 'Statistics\\'):
            os.mkdir(outputPath + 'Statistics\\')
        # 遍历所有区划并统计
        for district in districtName:
            ds = gdal.Open(outputPath + district + '.tif')
            rasterArray = ds.ReadAsArray()
            statistics = Counter(rasterArray.flatten())
            allClassCount = dict(landcoverDataset.count[rasterDataName])
            for i in list(dict(statistics).keys()):
                if i in list(allClassCount.keys()):
                    allClassCount[i] = statistics[i]
            pixel = sum(list(allClassCount.values()))
            # 面积(KM*2)
            area = pixel * pow((landcoverDataset.resolution[rasterDataName]) / 1000, 2)
            for key in list(allClassCount.keys()):
                allClassCount[key] = allClassCount[key] / pixel
                allClassCount[landcoverDataset.value_to_className[rasterDataName][key]] = allClassCount.pop(key)
            allClassCount['name'] = district
            allClassCount['area'] = area
            temp = pd.DataFrame(allClassCount, index=[allClassCount['name']])
            # 将数据写进dataframe
            table = table.append(temp, verify_integrity=True)
            ds = None
            count += 1
            if count % 50 == 0:
                print(f'thread {multiprocessing.current_process().name} is running...' + f'已完成：{count}/{len(shapeFilePath)}')
        # 把name列调到第一列
        name = table['name']
        table.drop(labels=['name'], axis=1, inplace=True)
        table.insert(0, 'name', name)
        table.to_csv(outputPath + 'Statistics\\' + 'districts_statistics.csv')
        print(f'thread {multiprocessing.current_process().name} is running...' + f'已完成：{count}/{len(shapeFilePath)}')


if __name__ == '__main__':
    # clipRaster("F:\\test\\GLC_FCS30_2020_E110N35.tif", "F:\\test\\南阳市-0377-411300-city\\", "F:\\test\\")
    # bound = shapeBound("F:\\test\\南阳市-0377-411300-city\\")
    # rasterList = glc2020Raster([1, 6, 9, 14], 'F:\\test\\')
    # rasterMosaic(rasterList, 'F:\\test\\', 'out2.tif')
    # clipDatasatRasterFromShp('F:\\投稿论文\\code\\data\\landcover\\GLC2020\\GLC\\',
    #                          'F:\\投稿论文\\code\\data\\ChineseDistrictShape\\武汉市-027-420100-city\\',
    #                          'F:\\投稿论文\\code\\data\\ChineseLandcover\\GLC2020\\')
    # ChineseDistrictsLandcover('glc_2017_10m_Tinghua')
    # ChineseDistrictsLandcoverStatistics('glc_2017_10m_Tinghua')
    # ChineseDistrictsLandcover('glc_2020_30m')
    # ChineseDistrictsLandcoverStatistics('glc_2020_30m')

    # 利用多进程处理1985、1990、1995、2000、2005、2010、2015年数据
    # pool = multiprocessing.Pool(4)
    # for datasetName in ['glc_2015_30m', 'glc_2010_30m', 'glc_2005_30m', 'glc_2000_30m', 'glc_1995_30m', 'glc_1990_30m', 'glc_1985_30m']:
    #     pool.apply_async(ChineseDistrictsLandcover, args=(datasetName, ))
    # pool.close()
    # pool.join()
    # for datasetName in ['glc_2020_30m', 'glc_2015_30m', 'glc_2010_30m', 'glc_2005_30m', 'glc_2000_30m', 'glc_1995_30m',
    #                     'glc_1990_30m', 'glc_1985_30m', 'glc_2017_10m_Tinghua']:
    #     pool.apply_async(ChineseDistrictsLandcoverStatistics, args=(datasetName, ))
    # pool.close()
    # pool.join()
    # print('end')

    # 单进程处理1985、1990、1995、2000、2005、2010、2015年数据
    # for datasetName in ['glc_2020_30m', 'glc_2015_30m', 'glc_2010_30m', 'glc_2005_30m', 'glc_2000_30m', 'glc_1995_30m',
    #                     'glc_1990_30m', 'glc_1985_30m', 'glc_2017_10m_Tinghua']:
    #     ChineseDistrictsLandcover(datasetName)
    # for datasetName in ['glc_2020_30m', 'glc_2015_30m', 'glc_2010_30m', 'glc_2005_30m', 'glc_2000_30m', 'glc_1995_30m',
    #                     'glc_1990_30m', 'glc_1985_30m', 'glc_2017_10m_Tinghua']:
    #     ChineseDistrictsLandcoverStatistics(datasetName)

    # 处理GlobaLand2000、2010、2020年数据
    # pool = multiprocessing.Pool(3)
    # for datasetName in ['GlobaLand30_2020', 'GlobaLand30_2010', 'GlobaLand30_2000']:
    #     pool.apply_async(ChineseDistrictsLandcover, args=(datasetName,))
    # pool.close()
    # pool.join()
    # pool = multiprocessing.Pool(3)
    # for datasetName in ['GlobaLand30_2020', 'GlobaLand30_2010', 'GlobaLand30_2000']:
    #     ChineseDistrictsLandcoverStatistics(datasetName)
    #     pool.apply_async(ChineseDistrictsLandcoverStatistics, args=(datasetName,))
    # pool.close()
    # pool.join()
    ChineseDistrictsLandcover('GlobaLand30_2010')
    ChineseDistrictsLandcoverStatistics('GlobaLand30_2010')
    print('end')
