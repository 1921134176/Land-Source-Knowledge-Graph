# #!/usr/bin/env python3
# # -*- coding: utf-8 -*-
#
# """
# 将中空院glc2020年数据重采样、重分类成与GlobaLand30_2020数据相同
# 需要使用arcgis的python2.7执行环境运行
# @author: ChengXin
# @contact:1921134176@qq.com
# @version: 1.0.0
# @license: Apache Licence
# @file: glc2020_resample_reclassfy.py
# @time: 2021/12/6 19:46
# """
# import arcpy
# import os
# from arcpy.sa import *
#
# arcpy.env.workspace = r".\\"
#
#
# def glc2020_resample_reclasspy(inputDir, outputDir, referenceDIr):
#     """
#     将中空院glc2020年数据重采样、重分类成与GlobaLand30_2020数据相同
#     :param inputDir: glc2020文件路径
#     :param outputDir: 输出文件夹
#     :param referenceDIr: 重采样参考的文件夹
#     :return:
#     """
#     inputDir = inputDir.decode('UTF-8')
#     outputDir = outputDir.decode('UTF-8')
#     referenceDIr = referenceDIr.decode('UTF-8')
#     inputRasterList = os.listdir(inputDir.encode('gb2312'))
#     inputRasterList = [i.decode('gb18030') for i in inputRasterList]
#     if not os.path.exists(outputDir):
#         os.mkdir(outputDir)
#     if not os.path.exists(os.path.join(outputDir, 'resample')):
#         os.mkdir(os.path.join(outputDir, 'resample'))
#     count = 0
#     for inputRasterName in inputRasterList:
#         if os.path.isfile(os.path.join(inputDir, inputRasterName)) \
#                 and not inputRasterName[0] in ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']:
#             in_raster = os.path.join(inputDir, inputRasterName)
#             out_raster = os.path.join(outputDir, 'resample', inputRasterName)
#             cell_size = "{0} {1}".format(arcpy.Describe(os.path.join(referenceDIr, inputRasterName)).meanCellWidth,
#                                          arcpy.Describe(os.path.join(referenceDIr, inputRasterName)).meanCellHeight)
#             if not os.path.exists(out_raster):
#                 arcpy.Resample_management(in_raster, out_raster, cell_size, "NEAREST")
#             if not os.path.exists(os.path.join(outputDir, inputRasterName)):
#                 remap = RemapRange([[0, 5, 0],
#                                     [10, 20, 10],
#                                     [50, 100, 20],
#                                     [120, 125, 40],
#                                     [130, 160, 30],
#                                     [180, 181, 50],
#                                     [190, 191, 80],
#                                     [200, 205, 90],
#                                     [210, 211, 60],
#                                     [220, 221, 100]])
#                 outReclass = Reclassify(out_raster, "Value", remap)
#                 outReclass.save(os.path.join(outputDir, inputRasterName))
#         count += 1
#         if count % 30 == 0:
#             print '已完成{}/{}'.format(count, len(inputRasterList))
#     print '已完成{}/{}'.format(count, len(inputRasterList))
#
#
# if __name__ == "__main__":
#     glc2020_resample_reclasspy('G:\\投稿论文\\code\\data\\ChineseLandcover\\GLC2020',
#                                'G:\\投稿论文\\code\\data\\ChineseLandcover\\GLC2020_resample_reclasspy_base_GlobaLand30_2020',
#                                'G:\\投稿论文\\code\\data\\ChineseLandcover\\GlobaLand30_2020')
#     print 'end'
