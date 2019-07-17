# BigData
利用Spring搭建基于Hadoop的大数据分析

## 根据老师给定的任务书实现具体功能部分
   - 目标：分析金庸小说中出现的人物和他们之间的关系
   - 源数据：全本的金庸武侠小说文集
   - MapReduce实现
       - 人名提取 NameSplit （基于Ansj_seg）
       - 人物同现统计 NameCount （不对关系做过多分析，认为同一段落的人物即存在关系）
       - 人物关系图构建 BuildRelationshipMap
       - todo 基于人物关系图的PageRank计算
       - todo 基于人物关系图的标签传播
       - todo 数据可视化
   - todo 可视化结果
   - 其他
   
## 自由发挥部分