# 能源结算流恢复事故：已有系统契约

本仓库是生产链路的缩小可执行模型，定位对象是流处理恢复协议，不是结算系统开发。
只诊断，不修复 flow.py，不改变既有用例、数据和断言；可新增调度、独立取证脚本及报告。
不依赖外部服务。三个 SQLite/内存状态域的生命周期由模型显式实现，recover 是规定的恢复入口；
不把杀操作系统进程作为本题要求，也不把模型中未实现的磁盘故障、分区或消息乱序当作事故原因。

## 数据与正确性

samples/case.json 是 UTF-8 JSON 对象，width 为固定正整数；transactions 中 lsn 从1连续递增。
每笔已提交源事务含 ops。op = {"table":名称,"id":字符串,"value":行对象或null}。
表及行：readings={device,day,amount}；devices={site}；sites={zone}。
day、amount 是整数；负数和零金额是结算冲正的合法数据；null 是删除；同主键后写覆盖，可删除、再建。
不要求诊断输入校验漏洞。提供的输入合法，新增证据也须保持这些类型与引用规则；
引用暂时缺失是合法状态，并非非法外键。事务内部按 ops 顺序执行，对外只核对完整源事务切点。

readings 按 owner(id,width) 唯一分片；devices、sites 广播到各片。
按 readings.device -> devices.site -> sites.zone 做内连接，每条可关联 reading 贡献
[fact_id,day,zone,amount]，重数恰为1，缺引用则无贡献。每日分区按 day、zone 汇总金额及笔数。
金额为0且笔数非0的组仍存在。总金额相等不能替代明细多重集或分组核对。
本地增量关联产生完整旧行的负权撤回和完整新行的正权插入，结果 bag 保留非零权重，包括负权重。
无故障时负权只在内部抵消，稳定切点输出应等于源前缀全量关联结果；既有 expected 独立从源数据计算，
不调用 Flow 的 contribution、checkpoint 或恢复函数。

## 调度入口

python3 incident.py replay samples/case.json incident
输出单个 JSON，匹配则 status=OK、退出0；业务或运行异常则 status=RECOVERY_CUT_MISMATCH、退出1。
expected 是该输出 cut 对应源前缀的明细多重集；bag 是实际结果；summary 行为 [day,zone,amount,count]。
输出还含 workers、receipts、checkpoints、trace、runtime_error、failed_step；键顺序不影响含义。
CHANNEL_GAP 是本系统运行期保护性异常，不等同于静默错账。checksum 正确也不构成业务一致性证据。
python3 incident.py demo samples/case.json incident 展示同一真实证据，但为演示固定退出0。
python3 incident.py normal 执行正常业务回归，成功输出 NORMAL_REGRESSION_OK。
python3 incident.py verify 保留正常回归，并独立运行事故两次核对完全相同的失败结果。
所有命令从仓库根目录执行，Python 3.9+ 标准库即可。

scenarios 的值为有序指令数组，新增 JSON 可直接用同一 replay 入口运行：
send(through)：将完整源事务前缀投递到各片接收队列；同片 FIFO，非重复、不丢包。
deliver(shard[,count])：执行指定片已接收的输入；省略 count 为全部。
dispatch()：将 worker outbox 中的包提交给 sink 的接收端；重复调度允许发生。
commit([count])：执行 sink 队列中的包，包内全部带符号变更在一个 SQLite 事务应用。
drain()：按片号 deliver 全部，再 dispatch、commit 全部；不改变源切点。
checkpoint(name)：发布该名字的检查点；新增调度应使用唯一名字。
recover(name)：模拟异常退出后恢复已发布检查点，关闭当前代次的在途工作并开启新 epoch。
对新增调度，count 为非负整数，shard 有效，send 不倒退，recover 的名字已经发布。
samples 中 quiet、fork_a/b/c/d、followup、continued 是同一事故数据上的重放分支；
它们的含义和是否足以隔离原因需由诊断者判定，不能依据名字给结论。

## 状态域和持久化边界

source 是不可变已提交日志。cut 表示发送过的完整源事务前缀，sent 是每片运输序号；
二者与工作者状态通过检查点保存。工作者 tables、applied、out_seq、outbox 属于检查点管理域。
源接收队列、sink 执行队列属于当前代次的在途内存，异常退出后不再存在。
sink.receipts 是独立持久的 SQLite 接收日志，包含 token、内容摘要、完整包体；
同 token 同内容返回已存在，同 token 不同内容抛 TOKEN_CONTENT_CONFLICT。
bag 是检查点管理的已发布投影，恢复按被选中的检查点内容重置。
检查点本体及 SHA256 属于持久的协调器域；checkpoint 返回后立即可用。
epoch 是协调器单调代次，recover 后递增，不回退；历史 outbox 包沿用原 token。
token=[产生时epoch,shard,片内output序号]，cause=[lsn,事务内op下标,片内input序号]。
不允许假设这些域之间存在未实现的共同事务、自动重送、队列备份或后台修补。
每个有效恢复切点的业务承诺是：源前缀关联与物化结果一致，并可继续合法输入而不因本次恢复产生漏算、
重复、残余负权或序号断裂。源日志和所有 receipts 均可取证，不要求在生产协议中自动修复。

## 诊断交付

交付 diagnosis.md，以及报告引用的新增 JSON 调度和原始 replay 输出。
报告必须引用最终源码文件与行号，将 source 接收、关联执行、输出接收、投影执行、检查点及恢复
串成可核对的偏序和状态证据；对相同症状的不同原因分别给出最小因果见证和只改变调度的反事实。
需要覆盖源端、关联端和接收日志端的恢复边界，以及首次恢复后再次检查点、继续输入与再次恢复的传播。
不能只列疑似代码、把全部未完成队列都判成已损失，或把一次观察到的错误扩写成所有恢复都会失败。
说明当前明细错误、分组错误、未来继续运行风险三种口径的准确范围，区分必然受影响与依赖后续数据的情况；
给出可用于其他合法输入和调度的精确影响判据，说明抵消、无关联和引用变化会如何改变可观察范围。
推广判据不能退化为每次全量重放源日志、比较 expected 和 bag；应从恢复边界的消息、包和依赖关系
推导已损失、可补回及继续运行时才暴露的影响，并说明多次恢复时如何组合与抵消。可对证据定位的局部
依赖重放；实际影响确实覆盖全量时不限制读取。报告给出判据、伪代码及完备性理由即可，不要求上线分析器。
全量 expected 仍可作为实验的独立校验，不能替代因果归属和影响范围推导。
解释检查点本体相同而恢复结果是否可能不同，并用真实调度证据支持结论。
报告中的事实结论应可通过提供的入口重跑；不能用修改业务实现之后通过来替代原始失败证据。
不要求实现修复、替换存储或新增线上功能。
