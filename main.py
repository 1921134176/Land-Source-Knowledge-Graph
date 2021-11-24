
"""
districts.py: 主要用于动态加载与获取中国行政区划
landcover.py: 加载数据集信息与数据集下载
rasterclip.py: 利用行政区划矢量数据裁剪数据集并进行统计
district_graph.py: 主要用于将统计数据导入知识图谱
convert.py: 主要用于网络地图坐标数据转换
gdal_merge.py: 基于gdal的栅格数据镶嵌
globaland30_2020.py: 利用arcpy对GlobaLand30数据集进行重投影与栅格镶嵌，需要在arcgis提供的python2.7环境中执行
"""


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
