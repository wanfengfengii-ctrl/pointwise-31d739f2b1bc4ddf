# 能源结算流恢复错账诊断报告

- 诊断对象：`flow.py`（流处理恢复协议模型）、`incident.py`（replay/回归入口）
- 诊断方式：只新增调度与只读取证，不修改任何业务实现（`flow.py`、`incident.py`、`samples/case.json`、既有场景与断言均未改动）
- 复现入口（仓库根目录执行）：
  - `python3 incident.py replay samples/case.json incident`（退出 1，`status=RECOVERY_CUT_MISMATCH`）
  - `python3 incident.py verify`（正常回归通过 + 两次事故结果逐字节相同，退出 0）
- 本报告引用的全部原始输出在 `evidence/`，新增调度在 `diag/schedules/`，只读取证脚本为 `diag/forensics.py`

---

## 0. 结论速览

检查点 SHA256 校验通过、总金额相等，都不能证明业务一致；本次错账是**恢复协议在三个持久化状态域之间缺少边界协调**，存在两个相互独立、可分别触发的丢失窗口：

| 机制 | 丢失对象 | 触发条件（充分必要的最小模式） | 现象 | 最小见证 |
|---|---|---|---|---|
| **A：接收日志/投影边界缺口** | 已被 sink 持久接收（有 receipt）但尚未物化进 bag 的输出包 | 包在检查点发布前通过 `dispatch()` 进入持久 receipt 日志，而 `commit()` 发生在检查点之后；随后 `recover()` 用检查点旧 bag 映像重置投影 | 静默错账：包永不重放（`accept` 返回 `fresh=false`），撤回/插入差额丢失 | `m_A_accepted_not_committed` |
| **B：源接收/关联边界缺口** | 已进入分片收件箱（计入 `sent`）但 worker 尚未 `deliver` 的源消息 | 检查点时某分片 `sent[s] > workers[s].applied`（各片交付不同步），随后 `recover()` 清空 inbox 且不重送 | 静默状态分叉；下次对该片 `send` 即抛 `CHANNEL_GAP`（保护性异常，非静默错账） | `m_B_inflight_silent` / `m_B_channel_gap` |

根因不在增量关联算法本身（无故障路径与正常回归全部正确），而在 `recover()`（`flow.py:185-197`）只做了 bag 映像恢复与代次递增，**没有把持久 receipt 日志、worker 已确认输入位点与恢复后的重放范围协调一致**。

---

## 1. 模型的状态域与持久化边界（源码定位）

| 状态域 | 载体 | 生命周期 | 源码 |
|---|---|---|---|
| source 切点 `cut`、运输序号 `sent` | Flow 内存，随检查点保存 | 检查点域 | `flow.py:80-84`、`flow.py:177` |
| 分片收件箱 `inbox` | 当前代次内存 | **异常退出即丢弃**，恢复时置空 | `flow.py:84`、`flow.py:194` |
| worker 表状态/`applied`/`out_seq`/`outbox` | 检查点域 | 随检查点整体保存/恢复 | `flow.py:90-93`、`flow.py:174-176`、`flow.py:192` |
| sink 接收日志 `receipts`（SQLite，token+摘要+完整包体） | 独立持久域 | **恢复不清理**，同 token 同内容幂等、异内容抛错 | `flow.py:34-35`、`flow.py:39-49` |
| sink 执行队列 `sink_inbox` | 当前代次内存 | **异常退出即丢弃**，恢复时置空 | `flow.py:86`、`flow.py:195` |
| 投影 `bag`（SQLite，带符号权重） | 检查点域 | 恢复时按选中检查点的映像整体重置 | `flow.py:54-65`、`flow.py:67-70`、`flow.py:193` |
| 检查点本体与 hash | 协调器持久域 | `recover` 校验 hash 后恢复 | `flow.py:173-183`、`flow.py:186-188` |
| epoch/token | 协调器代次 | recover 单调递增，历史包沿用原 token | `flow.py:147`、`flow.py:189` |

关键不对称：**receipts 跨恢复存活，bag 被回滚到检查点映像，两个在途队列被直接丢弃且无任何重放登记**。契约明确禁止假设未实现的共同事务、自动重送或队列备份（`CONTRACT.md:60`），因此这两个缺口不会被系统自行弥合。

---

## 2. 机制 A：接收日志与投影之间的丢失窗口

### 2.1 代码级因果链

1. `dispatch()` 把 worker outbox 的包交给接收端；`Journal.accept` 对新 token 持久化 receipt 并把包副本放进 `sink_inbox`（`flow.py:152-158`、`flow.py:39-49`）。
2. `checkpoint()` 快照 bag 映像；它**只按 token 过滤 worker outbox 里已被接收的包（`flow.py:176`），不要求已接收的包已经物化**。于是“已接收、未 commit”的包既不在检查点 outbox 里，其效果也不在检查点 bag 里。
3. `recover()` 执行 `sink.restore(body["bag"])`，把 bag 重置为检查点旧映像（`flow.py:193` → `flow.py:67-70`），同时把 `sink_inbox` 置空（`flow.py:195`）；receipts 原样保留。
4. 恢复后 worker 重新产出同一 token 的包时，`accept` 查到同 token 同内容，返回 `fresh=false`（`flow.py:43-46`），`dispatch` 不再把它放入执行队列（`flow.py:155-157`）。**包的效果被永久跳过**，而 bag 已被回滚——差额既不在 bag，也无法补回。

### 2.2 事故场景的偏序证据（`evidence/incident.out.json` 的 trace，事件号 n）

```
n=36  checkpoint C0（cut=1，bag 为 lsn1 前缀的 4 条 east/west 明细）
n=37-38 send lsn2 → 两片 seq=8（广播 devices/dA 改挂站点 C）
n=39  join.apply shard0 seq8：产出 token [0,0,3]（f0：east 100 撤回 / north 100 插入）
n=42  sink.accept token[0,0,3] fresh=true  ← 已进入持久 receipt
n=45  checkpoint C1（hash 80110eb7…）：bag_digest 与 C0 完全相同，
      即 C1 的 bag 映像不含 token[0,0,3] 的效果；kept=[0,0]
n=46  shard1 才交付 seq8（f1 的撤回/插入包 token[0,1,3] 此时才生成）
n=52  token[0,1,3] fresh=true 接收
n=53-54 sink.apply 两个 *3 包      ← commit 发生在检查点之后
n=55  restore C1：receipts=6 全部存活，bag 被重置回 C1 映像
（恢复后 drain 中两个 *3 包 accept 均 fresh=false，无任何 sink.apply）
```

最终 bag 停留在 lsn1 前缀：f0/f1 仍是 `east`，而源前缀 lsn2 的正确结果要求它们在 `north`（输出中 `expected` 独立计算，见 `incident.py:12-28`）。`runtime_error=null`，是**静默错账**。

### 2.3 只改调度的反事实隔离

仅把 `commit` 移到检查点之前，输入数据与其余指令顺序不变：

- 失败：`diag/schedules/case_diag.json::m_A_accepted_not_committed` → 退出 1，A 类 token `[0,0,3]、[0,1,3]`
- 反事实：`…::m_A_counterfactual_commit_first` → 退出 0，bag 与 expected 完全一致

原始输出：`evidence/case_diag__m_A_accepted_not_committed.out.json`、`evidence/case_diag__m_A_counterfactual_commit_first.out.json`。
同一隔离在既有场景上也成立：`fork_a`（先 commit 再 CP，失败由 B 主导但含 A）对比 `fork_d`（CP 时包仍在 outbox，成功）。

---

## 3. 机制 B：源接收与关联执行之间的丢失窗口

### 3.1 代码级因果链

1. `send()` 对广播类 op（devices/sites）向**所有分片**发消息并递增各片 `sent[s]`（`flow.py:104-110`）；各片交付由调度独立控制，天然可以不同步。
2. `checkpoint()` 保存的 worker 快照里 `applied` 是该片实际处理到的输入序号（`flow.py:174-182`）。当 `sent[s] > applied[s]` 时，差距部分只存在于 `inbox`。
3. `recover()` 把 `inbox` 直接置空（`flow.py:194`），**不重送、不记录缺口**；worker 仍停在旧 `applied`，而 `sent` 恢复为检查点值（`flow.py:191`）。
4. 此后两种后果：
   - **静默分叉**：丢失消息的效果从未发生，worker 表状态与源前缀不一致，且没有任何异常（事故 C1 时 shard1 `applied=7 < sent=8`，lsn2 的广播对 shard1 永久丢失）。
   - **CHANNEL_GAP（保护性异常）**：下次 `send` 在该片分配 `seq=sent[s]+1`，`deliver` 时序号不等于 `applied+1`，`flow.py:119-120` 抛错。这是显式保护，不等同于静默错账（`CONTRACT.md:31`）。

### 3.2 最小因果见证与反事实

- 静默：`m_B_inflight_silent`（CP 时只 deliver shard0；恢复后 drain，无异常但 f1 留在 east）→ 退出 1，`evidence/case_diag__m_B_inflight_silent.out.json`
- 序号断裂：`m_B_channel_gap`（同上，恢复后再 `send(3)`）→ `runtime_error=CHANNEL_GAP, failed_step=8`，`evidence/case_diag__m_B_channel_gap.out.json`；既有 `continued` 场景在 step 11 复现同一异常（`evidence/continued.out.json`，trace n=59）
- 反事实：`m_B_counterfactual_deliver_all`（检查点前两片都交付）→ 退出 0

`diag/forensics.py` 对事故 C1 的推导输出（`evidence/forensics_incident.json`）：
`lost_inflight_inputs = {"0": [], "1": [8]}`，`channel_gap_on_next_send = {"0": false, "1": true}`，
并通过**仅对丢失的 seq8 做局部依赖重放**得出本应产出的带符号变更
`[f1,east,-100] -1 / [f1,north,-100] +1`——无需重放全部源日志。

### 3.3 检查点保留 outbox 的路径不是漏洞

若丢的是 readings 分片消息或交付已完成但未 dispatch，产出包仍在 worker outbox；`checkpoint` 仅过滤已被 sink 接收的 token（`flow.py:176`），未接收的包随检查点保留，恢复后可正常投递。`fork_d`/`same_cp_safe_branch` 的成功（`kept=[1,1]`，token `[0,0,3]、[0,1,3]` 随 CP 存活）证明了这条路径健全——问题严格限定在两个“跨域未协调”窗口。

---

## 4. 相同检查点为何产生不同恢复结果

检查点 hash 只覆盖 `{cut, sent, workers, bag}` 这个 body（`flow.py:177-178`），**不覆盖 receipts 的物化进度、两个在途队列，也不覆盖检查点发布之后、恢复之前发生的 dispatch/commit 调度**。因此同一 hash 的检查点，在不同调度上下文中恢复，结果可以不同。

实证（`diag/schedules/case_diag.json`，原始输出 `evidence/case_diag__same_cp_*.out.json`）：

| 分支 | C1x hash | 恢复时 receipts | 恢复后 token[0,*,3] 的 accept | 结果 |
|---|---|---|---|---|
| `same_cp_safe_branch`（CP 后直接 recover） | `0eddfda9…` | 4（*3 未接收，留在 CP outbox） | `fresh=true`（n=43-44），正常 sink.apply | **OK** |
| `same_cp_unsafe_branch`（CP 后先 dispatch+commit 再 recover） | `0eddfda9…`（**完全相同**） | 6（*3 已接收） | `fresh=false`（n=51-52），效果被跳过 | **RECOVERY_CUT_MISMATCH** |

两个分支的检查点 body（含 bag、workers、outbox）逐字节相同，区别只在检查点之外的持久 receipt 域和恢复前调度。既有 `fork_c`（失败）与 `fork_d`（成功）是同一现象的另一对复现，C1 hash 同为 `0eddfda9…`。

结论：**checksum 正确只证明检查点映像自身未损坏，不证明它与 receipt 日志、源前缀之间一致**（`CONTRACT.md:31`）。

---

## 5. 影响范围：三个口径必须分开

### 5.1 当前明细口径（bag 多重集，最严格）

判据：凡落入 A/B 窗口的带符号变更，其涉及的明细行权重必然偏离源前缀；只要存在至少一个未弥合窗口，明细口径必然错误。事故中 f0、f1 两行错误（各缺一对 `-1/+1` 变更），f2/f3 不受影响。`incident.py:60` 以 bag 多重集判等，故标记失败。

### 5.2 分组口径（day, zone 汇总，`incident.py:31-39`）

分组是明细的折叠，错误是否在分组口径可见取决于数据：

- **跨 zone 的引用变更**：明细行整体换组，分组口径必然出现幽灵组/缺组。事故中 `(0,east)` 组残留、`(0,north)` 组缺失，金额与笔数同时错误（`evidence/incident.out.json` 的 `actual_summary` vs `expected_summary`）。
- **同组内冲正抵消**：`case_general.json::offset_hide` 中 r0=+40/r1=-40 同属 day5，丢包后 day5 两组金额都为 0，**金额差被抵消掩盖，但笔数与 zone 标签仍然错误**（`evidence/case_general__offset_hide.out.json`）。
- **零金额行**：r3 金额为 0，金额差恒为 0，但 `(7,z1)` 幽灵组笔数=1 仍暴露（契约规定金额 0 笔数非 0 的组仍存在，`CONTRACT.md:20`）。
- **总金额恒等是最弱口径**：每个增量包都是“完整旧行 -1、完整新行 +1”（`flow.py:137-143`），行金额不变时包内金额和为 0，因此本次全部失败运行总金额都相等（实测 incident/fork_*/followup/continued 均 `total actual == expected`）。总金额相等**不能**替代明细或分组核对。

### 5.3 未来继续运行口径（延迟暴露与不可自愈）

- **缺引用使影响暂时不可见**：丢失消息的 `affected` 集合（`flow.py:127-130`）若全部处于无关联状态（device/site 缺失），`contribution` 返回 None（`flow.py:19-27`），丢包当时不产出任何可观察差异；状态分叉潜伏，直到后续事务补齐引用才暴露为缺行/错行。`case_general.json::missing_ref_quiet` 中 dangling 读数 r2 即此模式；取证输出 `evidence/forensics_general__missing_ref_quiet.json` 的 `dangling_affected`/`currently_observable_facts` 给出了潜伏集合。
- **CHANNEL_GAP 阻断续跑**：B 窗口后任何对落后分片的 `send` 直接中断流水线（`continued` step 11、`m_B_channel_gap` step 8、`tombstone_recreate` step 16）。
- **检查点会冻结错误**：在错误状态上再次 `checkpoint`，错误 bag 被固化为新的恢复基线（followup 的 C2，hash `264a70b7…`，其 bag_digest 已不等于正确值），之后即使不再发生新故障，恢复也只能回到错误状态。
- **token 冲突隐患（依赖后续数据）**：token 为 `[epoch,shard,out_seq]`（`flow.py:147`），历史 outbox 包沿用旧 epoch。若恢复后新代次的 out_seq 再次走到与旧持久 receipt 相同的序号、而内容不同，`accept` 将抛 `TOKEN_CONTENT_CONFLICT`（`flow.py:44-45`）。本次数据未触发（同序号内容恰好相同，表现为 fresh=false 静默跳过），但这是同一边界缺陷在其他合法输入下的另一失效模式。

### 5.4 多次恢复如何组合与抵消（followup 实证）

`followup`（`evidence/followup.out.json`，两次 restore：n=50 receipts=6、n=60 receipts=8）：

1. 第 1 次恢复丢失 lsn2 对 f0/f1 的 zone 迁移；epoch1 处理 lsn3 时，worker 在**错误的基线状态**上计算增量，产出 token[1,*,4]，其撤回项是 `north,100/-100`（trace n=57-58）——撤回了一个 bag 里从未存在过的行，制造出 `-1` 残余负权。
2. C2 把含残余负权的 bag 固化；第 2 次恢复后 lsn4 的包（epoch2）同样在错误基线上增量。
3. 最终 bag 同时含：lsn1 的 east 幽灵正权行、被错误撤回的 north `-1` 行、lsn3 的 north 新行（见 `evidence/followup.out.json` bag，7 行 vs 正确 3 行）。

组合规则（带符号代数）：每次恢复把 bag 回滚到当时检查点映像，receipts 单调累积；窗口丢失的变更集 D 从 bag 中扣除但留在 receipt 折叠中，此后所有在错误基线上产生的增量都叠加额外误差 E_i。最终误差 = Σ(回滚掉的 D_k) + Σ(错误基线增量 E_i)，同键权重可代数抵消（followup 分组口径上 east 组金额 100+(-100)=0 被抵消，但 north 组笔数与残留负权仍在）。`diag/forensics.py` 用“receipt 全量包体折叠 vs 最终 bag”的差集精确给出 A 类足迹：followup 为 token `[0,0,1],[0,0,3],[0,1,1],[0,1,3],[1,0,4],[1,1,4]`（原始取证输出 `evidence/forensics_followup.json`）。

---

## 6. 可推广的精确影响判据（无需全量源重放）

对任意合法输入与调度，在每个检查点/恢复边界仅用消息、包、依赖关系计算（已实现于 `diag/forensics.py`，只读）：

### 6.1 边界集合定义

- A(C) = { token 为 fresh 的 sink.accept 事件号 < 检查点事件号，且其 sink.apply 事件号 > 检查点事件号（或不存在）} —— 检查点时“已接收未物化”的包。
- B_s(C) = { seq ∈ 该片 source.accept 序号 ∣ seq > worker_s.applied } —— 检查点时各片“已接收未关联”的输入消息。
- G(C) = 检查点 outbox 中保留的包 token（`flow.py:176` 过滤后的剩余），这是**可自动补回**部分。
- R = 恢复时刻存活的 receipt token 集合；恢复后重放 token t 的实际效果 ⇔ t ∉ R（fresh=true）。

### 6.2 影响判定伪代码

```
for 每个恢复点 C:
  # A 类：必然的当前明细误差（恢复后 fresh=false 即永久跳过）
  lost_A = [t in A(C) 且 t in R]               # 其 changes 全部从正确结果中缺失
  # B 类：对每条丢失消息做局部依赖重放（仅重放 B_s(C) 中的消息，
  #       初始状态用 C 的 worker 快照，关联规则与 flow.py:124-143 相同）
  for m in B_s(C):
      P(m) = 局部重放得到的带符号变更
      O(m) = P(m) 中 affected 事实在 C 快照下 contribution 非空的行   # 立即可观察
      H(m) = 其余行（缺引用/缺设备）                                 # 潜伏，引用补齐时暴露
  # 可补回：G(C) 中的包恢复后 fresh=true 正常应用，不计入误差
  # 续跑阻断：任一 s 满足 sent[s] > applied[s] ⇒ 下次 send 到 s 必抛 CHANNEL_GAP
  # 明细误差集 = lost_A 的 changes ⊎ 全部 P(m)，按 canonical(row) 做权重代数和
  # 分组误差 = 对误差集按 (day,zone) 折叠；同组金额可抵消，笔数/标签不抵消
  # 多恢复：各恢复点误差集按时间顺序做带符号并集；在错误检查点上发布的新 CP
  #         把累计误差冻结进基线，后续增量以该基线为起点叠加（见 5.4）
```

### 6.3 完备性理由

1. bag 的唯一写路径是 `materialize`（`flow.py:54-62`），其输入唯一来自 sink_inbox，而 sink_inbox 只由 `accept` 的 fresh 分支填入（`flow.py:155-157`）。因此一个包的效果能进入 bag，当且仅当它在某次 dispatch 时 fresh。恢复后 fresh 性完全由持久 receipts 决定（`flow.py:42-46`），故 A(C)∩R 穷尽了 A 类全部丢失，无遗漏也无误报。
2. worker 表状态的唯一写路径是 `deliver`（`flow.py:113-150`），严格按片内 seq 连续应用（`flow.py:119-120`）。inbox 在恢复时清空且无重送（`flow.py:194`），故 `sent[s]` 与 `applied[s]` 之间的序号差穷尽 B 类丢失消息；对这有限集合在检查点快照上做局部重放，即精确恢复其应有输出，不需要扫描源前缀。
3. 可观察性由 `contribution` 的 None 判定精确刻画（`flow.py:19-27`），潜伏项 H(m) 的暴露时机就是未来某个使引用链闭合的 op——无需预测未来数据，只需在新 op 到达时对 H 中相关事实重新判定。
4. 抵消是权重代数的自然结果：判据输出的是带符号误差多重集，分组/总额口径只是不同折叠函数，三种口径的可见性可由同一误差集机械推出。

---

## 7. 正常回归为何保持通过、事故边界在哪

`normal()`（`incident.py:68-90`）的每个切点都是 `send → drain → checkpoint → recover → drain`：`drain()` 内含完整 `dispatch()+commit()`（`flow.py:167-171`），检查点发布时 A(C)=∅；drain 又对所有分片全部 deliver，B_s(C)=∅。两个窗口都不打开，所以 width=1/2/3、全部切点及 tombstone/再建路径均通过（实测 `evidence/normal.out.txt`：`NORMAL_REGRESSION_OK`）。

**因此不能把结论扩写成“所有恢复都会失败”**：失败的充要条件是恢复边界上 A(C) 或 B_s(C) 非空。`quiet`、`fork_d` 以及全部正常回归用例证明：只要检查点时“已接收必已物化、已发送必已关联”，同一 `recover` 入口给出完全正确的结果。

---

## 8. 交付物清单与重跑方式

```
# 连续两次同标记失败（evidence/two_consecutive_failures.txt，两次均退出1、输出逐字节相同）
python3 incident.py replay samples/case.json incident
python3 incident.py replay samples/case.json incident
# 官方入口：正常回归 + 两次事故一致性断言（退出0）
python3 incident.py verify
# 只改调度的反事实与最小见证（退出码见各文件名对应记录）
python3 incident.py replay diag/schedules/case_diag.json m_A_accepted_not_committed   # 1
python3 incident.py replay diag/schedules/case_diag.json m_A_counterfactual_commit_first # 0
python3 incident.py replay diag/schedules/case_diag.json same_cp_safe_branch          # 0
python3 incident.py replay diag/schedules/case_diag.json same_cp_unsafe_branch        # 1
python3 incident.py replay diag/schedules/case_diag.json m_B_inflight_silent          # 1
python3 incident.py replay diag/schedules/case_diag.json m_B_channel_gap              # 1（CHANNEL_GAP）
python3 incident.py replay diag/schedules/case_diag.json m_B_counterfactual_deliver_all # 0
python3 incident.py replay diag/schedules/case_diag.json multi_recovery_propagation   # 1
# 独立合法输入上的推广证据（抵消、缺引用、零金额、tombstone 再建）
python3 incident.py replay diag/schedules/case_general.json offset_hide               # 1
python3 incident.py replay diag/schedules/case_general.json offset_hide_counterfactual# 0
python3 incident.py replay diag/schedules/case_general.json missing_ref_quiet         # 1
python3 incident.py replay diag/schedules/case_general.json tombstone_recreate        # 1（CHANNEL_GAP）
# 只读取证（不重放全量源，仅对边界丢失消息做局部依赖重放）
python3 -I diag/forensics.py samples/case.json evidence/incident.out.json
```

- `diag/schedules/case_diag.json`、`diag/schedules/case_general.json`：仅新增调度/独立合法输入，事务字段类型与引用规则符合 `CONTRACT.md:10-15`
- `evidence/`：8 个既有场景 + 12 个新增调度的原始 replay JSON、stderr、取证 JSON、`verify`/`normal` 原始输出、两次连续失败记录
- 未修改 `flow.py`、`incident.py`、`samples/` 及任何既有断言；按要求不提供修复
