* data_generator.py: 用于生成计算标签关系的基础数据
-comp_tag: 输入参数为新系统命中的全量公司标签结果和旧系统的标签结果表名，db是和数据库的连接；
           中间环节将公司id-name生成了字典以pickle形式储存为comp_id_name_dict.pkl供后面调用；
           返回值为公司-概念标签、公司-非概念标签各自的一一对应数据集合（comp_ctag_table, comp_nctag_table），以及仅含概念标签的全部信息comp_ctag_table_all_infos作为后续计算使用。

-data_aggregator：依赖上述输出对数据进行聚合；
                  中间环节为全量标签生成了hashcode作为唯一标识，储存在tag_dict.pkl二进制文件中；
                  以公司为单位聚合其概念和非概念标签以字典形式记录，储存在comp_tags_all.pkl二进制文件中；
                  记录概念标签之间的位置关系，作为计算筛选条件的依据，储存在ctag_position.pkl二进制文件中；
                  以标签为单位，聚合概念、非概念标签各自包含的公司列表，以及公司总数为返回值，作为后续计算依据。

* comp_property.py: 公司属性生成，考虑到后续可能有各类公司属性（或公司间关系生成的属性）作为筛选条件，因此单独作为一个计算模块，独立扩展。这里的版本只抽取了其最低层和最顶层概念标签作为计算股权关系、是否同产业链、产业树等条件的依据。

* data_calculator.py: 用于计算标签之间关联值（概念-概念、概念-非概念、非概念、非概念），分别储存在ctag_ctag.pkl、ctag_nctag.pkl、nctag_nctag.pkl三个二进制文件中。

* pipline.py: 将前面的数据准备、数据计算环节串联，可以统一计算生成更新数据。
-all_inputs_generator: 依次调用上述三个脚本，计算并储存全部数据
-data_loader: 从计算好的文本中加载公司推荐函数需要调用的数据，将公司property字典和公司标签字典拼接（这样property依赖可以在计算的时候独立扩展）

* recommendation.py: 以公司名称为输入，返回关联最强的n家公司。
-cal_tag_cartesian: 两个标签列表的笛卡尔积。
—cal_tags_link: 两个标签列表的三组关联值。
-cal_company_dis: 计算某公司和列表内其余公司的相关性值。

-concept_tree_relation: 计算两公司是否在同一产业树、处于上下游。
-branch_stock_relation: 计算两公司是否三级之内有股权关系、是否互为分支机构。
以上二者如果全量计算完储存，量级有些大，所以是依赖于先前存好的概念标签关系，和图数据库，实时查询计算的。
（以上五个为中间计算函数，可不需要详细关注）

-multi_process_rank: 计算公司关联度调用的主要函数，输入为comp_name-公司名称、graph-图数据库连接，weights-三种标签关联的权重tuple（已预设默认值），response_num-返回的top N数目（默认为None，即返回全部公司的关联值降序排列结果），计算时对三组标签关系值的过滤阈值（已预设默认值），process_num-多进程并发的数目。
由于每次计算需要和其余约190万公司进行匹配计算，单个计算时间为3 min，为了提升计算效率，这里采用了简单的多进程，能将计算时间缩短为原有三分之一，为1 min左右。



由于计算需要调用离线储存的计算好的二进制文件，数据的读取需要一定时间，因此最好数据一次读取后放在缓存中，接口每次调用时不需要进行数据的重新加载（因为使用了多进程，没能将模型写成class形式）。另外这里的二进制文件都是用的相对路径，根据需要修改。
pipline.all_inputs_generator()函数仅需要在源数据发生了变化更新的时候（打标签数据被更新且通过，公司属性变化等）调用进行重新计算以生成新的数据储存，否则不需要使用。


（-api_entry.py: 自己用flask写的一个测试api接口，根目录下nohup python Code/api_entry.py启动服务，通过python的requests.post("http://172.29.237.212:8999/recommentdation", data={"comp_name": "深圳市移联网络科技有限公司", "response_num": 10})可以成功调用，不知道是否有更优的方式，因为计算时候需要多进程，因此没有考虑批量请求。）