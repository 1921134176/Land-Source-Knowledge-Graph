# #!/usr/bin/env python3
# # -*- coding: utf-8 -*-
#
# """
# 结合知识图谱和arcpy对数据进行拼接与统计
# 需要使用arcgis的python2.7执行环境运行
# @author: ChengXin
# @contact:1921134176@qq.com
# @version: 1.0.0
# @license: Apache Licence
# @file: get_raster_arcgis.py
# @time: 2021/12/1 17:48
# """
# import arcpy
# import json
# import os
#
# chineseLandCoverPath = 'G:\\投稿论文\\code\\data\\ChineseLandcover\\'.decode('UTF-8')
# landCoverList = os.listdir(chineseLandCoverPath.encode('gb2312'))
#
#
# def mosaicCityOrProvince(name, savePath):
#     arcpy.env.workspace = r".\\"
#     sr = arcpy.SpatialReference(4326)
#     with open(os.path.join(savePath, name + '_CityOrProvince', 'node.json').decode('UTF-8'), 'r') as f:
#         text = f.read().decode('UTF-8')
#     nodeDict = json.loads(text, encoding='utf-8')
#     nodeList = nodeDict.values()
#     for i in landCoverList:
#         inputRasterList = [os.path.join(chineseLandCoverPath, i, node)+'.tif' for node in nodeList[0]]
#         arcpy.MosaicToNewRaster_management(inputRasterList,
#                                            os.path.join(savePath, name + '_CityOrProvince'),
#                                            name+"_"+i+'.tif',
#                                            sr,
#                                            "8_BIT_UNSIGNED", "#", "1", "MAXIMUM", "FIRST")
#
#
#
# if __name__ == "__main__":
#     mosaicCityOrProvince(name='武汉市', savePath='G:\\投稿论文\\paper_content\\pic\\')
#     print 'end'
