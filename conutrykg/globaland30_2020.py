# #!/usr/bin/env python3
# # -*- coding: utf-8 -*-
#
# """
# 用arcpy处理GlobaLand30数据集，包括数据重投影与栅格镶嵌
# 需要使用arcgis的python2.7执行环境运行
# @author: ChengXin
# @contact:1921134176@qq.com
# @version: 1.0.0
# @license: Apache Licence
# @file: globaland30_2020.py
# @time: 2021/11/23 13:33
# """
# import arcpy
# import os
# import multiprocessing
#
#
# def mosaic(rasterPath, outdir, year):
#     arcpy.env.workspace = r".\\"
#     sr = arcpy.SpatialReference(4326)
#     # year = 2020
#     root = 'G:\\投稿论文\\code\\data\\ChineseLandcover\\GlobaLand30_2020'.decode('UTF-8')
#     rasterPath = rasterPath.decode('UTF-8')
#     # rasterPath = 'G:\\投稿论文\\code\\data\\landcover\\GlobaLand30_2020\\GLC\\'.decode('UTF-8')
#     # outdir = ".\\GlobaLand30_2020"
#     rasterList = os.listdir(root.encode('gb2312'))
#     count = 0
#     for raster in rasterList:
#         if raster[0] in ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']:
#             input_raster_list = []
#             left, down, right, up = raster.split('-')
#             left = int(left)
#             right = int(right)
#             down = int(down)
#             up = int(up[:-4])
#             for i in range(left, right+1):
#                 for j in range(down, up, 5):
#                     if j == 0:
#                         fileName = rasterPath + 'N{}_00_{}LC030\\'.format(i, year) + 'n{}_00_{}lc030_reproject_wgs84.tif'.format(i, year)
#                     elif j == 5:
#                         fileName = rasterPath + 'N{}_05_{}LC030\\'.format(i, year) + 'n{}_05_{}lc030_reproject_wgs84.tif'.format(i, year)
#                     else:
#                         fileName = rasterPath + 'N{}_{}_{}LC030\\'.format(i, j, year) + 'n{}_{}_{}lc030_reproject_wgs84.tif'.format(i, j, year)
#                     if os.path.exists(fileName):
#                         input_raster_list.append(fileName.encode('utf-8'))
#             if os.path.exists(outdir + "\\{}".format(raster)):
#                 count += 1
#                 continue
#             arcpy.MosaicToNewRaster_management(input_raster_list, outdir, "{}".format(raster),
#                                                sr, "8_BIT_UNSIGNED", "#", "1", "MAXIMUM", "FIRST")
#             count += 1
#         print 'thread {} is running...'.format(multiprocessing.current_process().name) + '已处理{}/104'.format(count)
#
#
# def reproject_wgs84(root):
#     arcpy.env.workspace = ".\\"
#     sr = arcpy.SpatialReference(4326)
#     root = root.decode('UTF-8')
#     listDir = os.listdir(root.encode('gb2312'))
#     count = 0
#     flag = 0
#     for file in listDir:
#         count += 1
#         # 只处理中国区域
#         if 42 < int(file[1:3]) < 54 and 0 <= int(file[4:6]) <= 55 and file[0] == 'N':
#             inputPath = os.path.join(root, file, file.lower() + '.tif')
#             outputPath = os.path.join(root, file, file.lower() + '_reproject_wgs84.tif')
#             if os.path.exists(outputPath):
#                 continue
#             arcpy.ProjectRaster_management(inputPath, outputPath,
#                                            sr, "NEAREST", "#", "#", "#", "#")
#             flag += 1
#         if count % 50 == 0:
#             print 'thread {} is running...'.format(multiprocessing.current_process().name) + '已处理{}/{}'.format(count, len(listDir))
#     print 'thread {} is running...'.format(multiprocessing.current_process().name) + '被重投影的文件数：{}'.format(flag)
#
#
# if __name__ == "__main__":
#     pool = multiprocessing.Pool(2)
#     pool.apply_async(reproject_wgs84, args=('G:\\投稿论文\\code\\data\\landcover\\GlobaLand30_2000\\GLC\\',))
#     pool.apply_async(reproject_wgs84, args=('G:\\投稿论文\\code\\data\\landcover\\GlobaLand30_2010\\GLC\\',))
#     pool.close()
#     pool.join()
#     pool.apply_async(mosaic, args=('G:\\投稿论文\\code\\data\\landcover\\GlobaLand30_2000\\GLC\\', ".\\GlobaLand30_2000", 2000))
#     pool.apply_async(mosaic, args=('G:\\投稿论文\\code\\data\\landcover\\GlobaLand30_2010\\GLC\\', ".\\GlobaLand30_2010", 2010))
#     pool.close()
#     pool.join()
#     print 'end'