# 基于GPT-5.5模型的VOC实体抽取/主题聚合方案调研

## 需求描述
我我一个游戏运营策划，目前正在针对游《信长之野望 真战》游戏进行用户行为研究，我需要从大量的用户原声数据中抽取提炼玩家的反馈信息和意图，通过了解玩家的诉求来发现问题、改进和优化游戏的体验，请你先针对《信长之野望》系列的SLG游戏的特点，搜索网上大量的相关游戏的知识或者该游戏的不同系统的知识，然后帮我分析整理一套系统化的分析框架，来洞察用户行为、挖掘用户原生信息中的价值。我们目前正在尝试的一个技术路线说明：我们针对收集获取到的大量的用户原声(包括公司游戏内的数据、外部公共域信息)，目前已经使用了AI LLM 对不同的用户原生进行了小结归纳，目前每一条用户原生数据都有一些抽取出来的重要实体和一条总结性的话题描述，后续我们想基于每条数据抽取出来的关键实体信息和话题描述，从不同的层级对不同的数据点进行向上的话题聚合，来方便后续的分析，我们想要一个具有严谨层次结构的从实体到最终不同层级分析唯独的框架大纲，能尽量涵盖当前SLG 游戏的大部份核心的用户原声信息。你可以现在GitHub上检索看看是否有类似的开源项目可以借鉴，然后基于上面的详细的需求描述进行深入的技术分析，并研究后续的落地方法。

# 方案调研

下面这套方案我会按“**游戏系统理解 → 用户原声分析框架 → 话题聚合技术路线 → 落地数据结构与流程**”来组织。

## 1. 先明确《信长之野望 真战》的用户反馈边界

从公开资料看，《信长之野望 真战》不是单纯传统单机《信长之野望》移植，而是《信长之野望·新生》授权、由《三国志·战略版》制作团队打造的“战国 SLG × RPG”手游。它的官方描述里同时强调了城主身份、个人六大技艺、家臣团、势力/一门、资源获取、兵种克制、战法搭配、攻城与多人协作等系统。([Google Play][1])

这意味着用户原声不能只按“战斗/养成/付费/BUG”四类粗分。它至少有三层体验叠加：

第一层是**传统光荣历史模拟 SLG 体验**：大名、城、郡、家臣、领地经营、政策、内政、外交、战争。原作《新生 / Awakening》的官方手册里，玩家目标就是作为大名经营国家、分封领地给家臣，并通过合作或战争扩张势力；日本地图按国、郡、城组织，控制郡会带来金钱与物资收益。([KOEI TECMO AMERICA][2])

第二层是**赛季制多人 SLG 体验**：资源发育、开荒、配将、兵种克制、攻城、联盟/一门组织、赛季结算、霸业目标、服务器生态。公开评测也提到，一门/公会协作是大规模模拟游戏的核心，大型据点攻城往往不是单人能完成的，弱战力玩家也能通过攻城器械、集结、组织行动发挥价值。([電ファミニコゲーマー – ゲームの面白い記事読んでみない？][3])

第三层是**手游化与 RPG 化体验**：抽卡/招募、个人技艺、小游戏、日常任务、付费公平性、活动节奏、新手引导、长线留存。官方 Google Play 介绍强调六大能力“武艺、辩才、茶道、算术、锻冶、建筑”，以及“不卖资源不卖兵，全靠配将思路和战术取胜”的公平对抗表达。([Google Play][1])

所以你们的用户原声分析框架，核心不是“把评论聚成几个主题”，而是要还原玩家在不同系统、不同成长阶段、不同社交组织位置下的**诉求、阻塞点、情绪与行为动机**。

---

## 2. 可借鉴的开源项目：没有完全现成的 SLG 运营分析系统，但组件很成熟

我检索下来，GitHub 上没有看到一个专门面向“SLG 游戏运营用户原声洞察”的完整开源项目；但有几类项目非常适合组合成你们的技术底座。

| 类型           | 可借鉴项目                                                  | 适合借鉴的点                                                                                                      |
| ------------ | ------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------- |
| 主题建模 / 话题聚合  | **BERTopic**                                           | 使用 Transformer embedding + c-TF-IDF 生成可解释主题，适合把大量文本聚成可命名话题。([GitHub][4])                                    |
| 层级话题建模       | **BERTopic hierarchical topics**                       | 官方文档说明可用 topic-term matrix、cosine similarity 和 linkage 建模主题层级，用来辅助“子话题 → 上位话题”合并。([maartengr.github.io][5]) |
| LLM 增强主题建模   | **TopicGPT**                                           | 用 LLM 生成更可读、更可编辑的动态话题表示；其聚类流程使用 UMAP 降维、HDBSCAN 自动找簇，也可指定话题数量后做凝聚聚类。([GitHub][6])                           |
| 短文本意图聚类      | **chat-intents**                                       | 面向短对话意图，使用 UMAP + HDBSCAN，并自动为短文本簇打标签，和“用户原声短句/客服句子”场景接近。([GitHub][7])                                      |
| 语义主题与检索      | **Top2Vec**                                            | 自动发现主题数量，支持短文本、层级主题、按关键词/主题检索文档，适合做探索性分析。([GitHub][8])                                                      |
| 关键词/实体补充     | **KeyBERT**                                            | 用 BERT embedding + 余弦相似度抽取最能代表文本的关键词/短语，可作为 LLM 实体抽取的补充或校验。([GitHub][9])                                    |
| LLM + 调研自由文本 | **AIAugmentedSurveyResponseCategorization**            | 其 README 明确展示了“自由文本 → embedding → 层级聚类 → LLM 辅助分类/主题”的流程，和你们的“用户原声上卷”很接近。([GitHub][10])                     |
| 客户反馈工程样例     | **feedback-analyzer / use-case-llm-customer-feedback** | 可借鉴 LangChain4j/Quarkus 前后端样例，或 Airflow + LLM + OpenSearch 的 MLOps 管道化思路。([GitHub][11])                     |
| 人工校验 / 标注闭环  | **Label Studio / Argilla / doccano**                   | Label Studio 支持多模态标注与导出；Argilla 支持过滤、AI 建议、语义搜索；doccano 适合文本分类、序列标注、摘要等 NLP 标注任务。([GitHub][12])             |

我的判断是：**不要直接押宝单一 topic modeling 工具**。你们已有 LLM 小结、实体抽取和话题描述，最适合的是“**领域本体 + LLM 结构化抽取 + embedding 聚类 + 人工闭环校准**”的混合路线。

---

## 3. 一套适合 SLG 用户原声的层级分析框架

我建议你们把每条用户原声拆成四类对象：

**实体 Entity**：玩家说到了什么。
例如：武将、战法、兵种、资源、天守、威信、汉方药、一门、攻城、赛季、抽卡、服务器、活动、BUG、充值礼包。

**事件 Event**：玩家经历了什么。
例如：开荒失败、抽不到核心武将、攻城集合失败、资源卡住、赛季被强盟碾压、战法触发不符合预期、登录闪退。

**意图 Intent**：玩家希望什么。
例如：求攻略、吐槽不公平、请求补偿、要求削弱、要求加强、反馈 BUG、表达流失、认可设计、比较竞品。

**分析维度 Dimension**：这条反馈最终应该服务于哪类产品决策。
例如：数值平衡、商业化、公平性、新手留存、社交组织、赛季生态、技术质量、活动节奏、IP 沉浸。

你们后续的话题聚合，应该从“文本相似”升级为“**实体-事件-意图-维度**的多视角聚合”。

---

## 4. 推荐的严格层级结构：从原声到决策维度

### L0：原始用户原声层

保留原文，不要只保留 LLM 总结。

字段建议：

```json
{
  "voice_id": "uuid",
  "raw_text": "原始玩家发言",
  "source": "in_game_feedback / customer_service / discord / facebook / app_store / bahamut / forum",
  "language": "zh-Hant / zh-CN / ja / en",
  "server_id": "S12",
  "season": "S1 / S2 / PK",
  "player_stage": "newbie / midgame / late-season / returning",
  "player_segment": "nonpayer / lightpayer / whale / alliance_leader / casual / hardcore",
  "event_time": "2026-xx-xx",
  "game_version": "x.y.z"
}
```

关键点：**原声必须能回溯**。LLM 小结会丢失情绪强度、讽刺、具体证据、玩家措辞，这些恰恰是运营分析里很有价值的信号。

---

### L1：实体层 Entity Layer

建议实体分 12 大类：

| 实体大类     | 示例                                               |
| -------- | ------------------------------------------------ |
| 系统模块     | 开荒、内政、资源、建筑、武将、战法、兵种、地图、行军、攻城、一门、赛季、活动、排行榜、商店、客服 |
| 战国 IP 实体 | 织田、德川、武田、上杉、伊达、丰臣、茶道、剑术流派、名城、历史事件                |
| 武将实体     | 具体武将名、稀有度、阵营、家族、属性、定位                            |
| 战斗实体     | 兵种、克制、战法、阵容、士气、地形、战报、攻城器械                        |
| 资源实体     | 铜钱、粮草、兵力、体力、汉方药、装备、信物、礼包                         |
| 社交实体     | 一门、当主、盟友、敌盟、指挥、集结、管理、退盟                          |
| 赛季实体     | S1/S2/PK、霸业、结算、出生州、迁城、合服、赛季重置                    |
| 付费实体     | 抽卡、月卡、首充、礼包、保底、返利、付费差距                           |
| 技术实体     | 卡顿、闪退、延迟、登录、掉线、机型、网络、外挂                          |
| 运营实体     | 活动、公告、补偿、客服响应、更新、削弱/加强                           |
| 情绪实体     | 爽、肝、累、被碾压、无聊、不公平、看不懂、想退游                         |
| 竞品实体     | 三国志战略版、率土之滨、其他 SLG、单机信长系列                        |

实体层要做**标准化**：
“抽卡 / 招募 / 寻访 / 登庸 / 出货 / 保底”可能都应该映射到 `monetization.gacha_recruit`；“一门 / 联盟 / 公会 / guild”应映射到 `social.alliance`。

---

### L2：原子反馈层 Atomic Feedback

一条原声常常包含多个诉求，必须拆成多个 atomic feedback。

例子：

> “新手引导还可以，但 S2 一进去就被大盟打爆，抽不到核心将，感觉不氪没法玩。”

应拆为：

```json
[
  {
    "claim": "新手引导体验较好",
    "sentiment": "positive",
    "intent": "praise",
    "entities": ["newbie_tutorial"]
  },
  {
    "claim": "S2 被强盟压制导致体验差",
    "sentiment": "negative",
    "intent": "complaint",
    "entities": ["season_s2", "alliance_power_gap", "pvp_suppression"]
  },
  {
    "claim": "抽不到核心武将导致认为不氪无法玩",
    "sentiment": "negative",
    "intent": "fairness_complaint",
    "entities": ["gacha", "core_officer", "pay_to_win"]
  }
]
```

这是非常关键的一步。否则你们后续聚类会把“新手引导好”和“赛季被打爆”错误聚到同一条文本里。

---

### L3：问题/诉求节点 Issue Node

这是第一层可运营分析的话题单元。

Issue Node 的命名应该是“**对象 + 问题 + 玩家影响**”，不要只写“武将问题”“资源问题”。

推荐命名方式：

```text
[系统对象] + [具体问题] + [玩家影响]
```

例如：

| 不推荐  | 推荐                      |
| ---- | ----------------------- |
| 武将问题 | 核心武将获取困难导致阵容成型慢         |
| 一门问题 | 弱一门组织力不足导致成员流失          |
| 赛季问题 | S2 初期强弱势力差距过大导致低战玩家失去目标 |
| 攻城问题 | 攻城集合时间要求高导致轻度玩家无法参与     |
| 资源问题 | 铜钱多系统共用导致技能养成与武将养成冲突    |
| 新手问题 | 兵种克制/战法联动解释不足导致新手配将困难   |

---

### L4：系统话题层 System Topic

建议把《信长之野望 真战》的用户原声划成 13 个一级系统话题。

| 一级话题           | 二级话题示例                         | 常见用户意图            |
| -------------- | ------------------------------ | ----------------- |
| 1. 新手与认知负荷     | 新手引导、开荒教学、配将理解、战报解释、系统入口复杂度    | 看不懂、求攻略、称赞友好、前期退坑 |
| 2. 发育与资源循环     | 土地、建筑、资源产出、体力、汉方药、NPC 信物、技艺小游戏 | 卡资源、嫌肝、节奏太慢/太快    |
| 3. 武将与阵容养成     | 武将获取、稀有度、阵营/家族加成、等级、装备、技能槽     | 抽不到、阵容不成型、养错成本高   |
| 4. 战法与数值平衡     | 战法触发、伤害、控制、治疗、兵种克制、战报          | 要削弱/加强、质疑随机性、求最优解 |
| 5. 地图、行军与攻城    | 地形、士气、行军时间、城池耐久、攻城器械、集结        | 参与门槛高、协作爽点、时间压力   |
| 6. 一门/联盟与社交组织  | 当主管理、指挥、成员活跃、外交、退盟、合盟          | 找组织、抱怨管理、被强盟压制    |
| 7. 赛季与服务器生态    | S1/S2/PK、霸业、出生州、赛季重置、合服、滚服     | 不公平、被碾压、目标断层、回流   |
| 8. 活动与运营节奏     | 七日活动、限时活动、签到、节日活动、补偿、公告        | 奖励不足、太肝、错过焦虑      |
| 9. 商业化与公平性     | 抽卡、礼包、月卡、保底、资源售卖、付费差距          | 氪金压力、P2W 争议、性价比   |
| 10. IP、历史与沉浸感  | 武将立绘、史实事件、台词、日式战国氛围、角色扮演       | 喜欢 IP、考据吐槽、沉浸感不足  |
| 11. 技术质量与体验    | 闪退、卡顿、发热、延迟、登录失败、战报异常          | BUG 反馈、请求修复、索赔    |
| 12. 安全与生态治理    | 外挂、脚本、多开、工作室、买号、辱骂             | 举报、要求封禁、质疑公平      |
| 13. 流失、回流与竞品比较 | 退游、回归、转服、竞品迁移、朋友带玩             | 表达离开、比较竞品、等待优化    |

这 13 类基本能覆盖 SLG 手游的大部分用户原声，同时保留《信长之野望 真战》的特征：**一门攻城、赛季霸业、配将战法、个人技艺、战国 IP 沉浸**。

---

### L5：产品分析维度 Decision Dimension

最后一层不要再叫“话题”，而应该叫“决策维度”。它是给策划、运营、数值、商业化、客服、技术团队使用的。

我建议顶层维度如下：

1. **留存风险**：新手劝退、赛季中后期目标断层、低战玩家无参与感。
2. **付费公平感**：抽卡、礼包、核心武将、战法获取、付费差距。
3. **策略深度与可解释性**：配将、兵种、战法、战报、克制关系是否可理解。
4. **社交组织健康度**：一门活跃、管理负担、指挥协作、弱盟生存。
5. **赛季生态健康度**：强弱分化、霸业目标、出生州平衡、合服/滚服问题。
6. **资源与成长节奏**：发育速度、资源瓶颈、体力、建筑、技艺小游戏负担。
7. **战斗与数值平衡**：武将强度、战法触发、兵种克制、攻城体验。
8. **内容与活动满意度**：活动奖励、任务压力、节日运营、补偿预期。
9. **技术稳定性**：卡顿、闪退、延迟、设备兼容、战报异常。
10. **IP 与沉浸价值**：历史还原、武将美术、台词、战国氛围、RPG 角色感。
11. **社区舆情与信任**：公告透明度、客服响应、版本争议、玩家对运营的信任。
12. **安全与公平治理**：外挂、脚本、工作室、代练、多开、恶意联盟行为。

GitHub 自身的客户反馈实践也强调，聚类反馈的价值在于更快识别重复痛点、改进优先级，把社区最需要的问题转化到产品路线图中。这个思路很适合你们把“玩家原声聚类”接到“策划动作优先级”。([The GitHub Blog][13])

---

## 5. 推荐技术路线：Top-down 本体 + Bottom-up 聚类

你们现在已经有“每条原声的重要实体 + 总结性话题描述”。下一步不要直接拿这些 summary 做聚类，而是建议做一个四通道表示。

### 5.1 单条数据的统一表示

每条 atomic feedback 生成：

```json
{
  "atomic_id": "uuid",
  "voice_id": "source voice id",
  "raw_quote": "玩家原句证据",
  "llm_summary": "一句话总结",
  "normalized_entities": [
    {
      "name": "抽卡",
      "type": "monetization.gacha",
      "canonical_id": "monetization.gacha_recruit"
    },
    {
      "name": "核心武将",
      "type": "officer.core"
    }
  ],
  "intent": "fairness_complaint",
  "sentiment": "negative",
  "severity": 4,
  "player_stage": "season_mid",
  "system_topic_l1": "武将与阵容养成",
  "system_topic_l2": "核心武将获取",
  "embedding_text": "抽不到核心武将导致阵容成型困难，并产生不氪无法玩的公平性质疑",
  "source_weight": 0.8,
  "confidence": 0.86
}
```

这里的 `embedding_text` 不建议只用原文，也不建议只用 LLM 总结，而是拼接：

```text
原声摘要 + 规范实体 + 意图 + 玩家阶段 + 系统上下文
```

例如：

```text
核心武将获取困难；抽卡/登庸；阵容成型；公平性质疑；S2中期；轻氪玩家
```

这样聚类会比单纯文本 embedding 更稳定。

---

### 5.2 聚合流程

推荐流程：

```text
原声采集
  ↓
清洗去重 / 语言归一 / 垃圾文本过滤
  ↓
LLM 多标签结构化抽取
  ↓
实体标准化 + 领域本体映射
  ↓
原子反馈拆分
  ↓
一级系统话题分类
  ↓
每个一级话题内部做 embedding 聚类
  ↓
LLM 生成簇标题、边界定义、代表样本
  ↓
人工审核：合并 / 拆分 / 重命名 / 挂载到层级树
  ↓
沉淀为稳定话题库 + 新兴话题发现池
  ↓
看板：趋势、严重度、玩家分群、版本对比、行动建议
```

核心原则：
**先按领域大类分桶，再在桶内聚类。**

不要把所有用户原声直接扔进一个向量空间聚类。因为“抽卡不公平”“攻城不公平”“服务器不公平”“外挂不公平”在 embedding 上可能非常近，但它们对应的是完全不同的策划和运营动作。

---

### 5.3 话题聚类算法建议

你们可以用三套策略并行，然后人工评估哪套效果最好。

#### 方案 A：BERTopic / UMAP / HDBSCAN

适合探索未知话题。
流程是：

```text
embedding → UMAP 降维 → HDBSCAN 聚类 → c-TF-IDF 关键词 → LLM 话题命名
```

BERTopic 本身就是使用 Transformer embedding 和 c-TF-IDF 生成可解释主题，且支持层级话题分析。([GitHub][4])

优点：自动发现主题，适合外部社区原声。
缺点：短文本、噪声、跨语言、版本上下文不足时容易出碎簇。

#### 方案 B：领域标签监督 + 聚类

先用你们设计的 13 个一级话题做 LLM/分类模型多标签分类，然后每个一级话题内部聚类。

```text
L1 分类 → L2 候选分类 → 桶内聚类 → LLM 命名
```

优点：话题结构稳定，适合运营看板。
缺点：容易漏掉新兴问题，所以要保留“未知/新兴话题池”。

#### 方案 C：实体图谱 + 社区发现

构建一个异构图：

```text
用户原声节点
实体节点
意图节点
系统节点
版本节点
服务器节点
玩家阶段节点
```

边包括：

```text
feedback - mentions -> entity
feedback - has_intent -> intent
feedback - belongs_to -> system
feedback - occurred_in -> version/season
entity - related_to -> entity
```

然后对反馈节点做图聚类或社区发现。
这条路线特别适合 SLG，因为 SLG 的问题往往不是单文本相似，而是由“实体共现 + 场景 + 玩家阶段”决定的。例如：

```text
S2 + 弱盟 + 出生州 + 被强盟压制 + 退游
```

这组信号未必文本相似，但产品含义高度一致。

---

## 6. “实体 → 话题 → 洞察”的推荐层级树

可以落成如下树形结构：

```text
D0 原始原声
 └── D1 原子反馈
      ├── E 实体层
      │    ├── 系统实体
      │    ├── 武将/战法/兵种实体
      │    ├── 社交/赛季实体
      │    ├── 资源/付费实体
      │    └── 技术/运营实体
      │
      ├── I 意图层
      │    ├── 抱怨
      │    ├── 建议
      │    ├── 求助
      │    ├── 认可
      │    ├── 举报
      │    ├── 流失表达
      │    └── 竞品比较
      │
      ├── T 话题层
      │    ├── 一级系统话题
      │    ├── 二级具体问题
      │    └── 三级问题簇
      │
      └── B 决策层
           ├── 影响指标
           ├── 受影响玩家群
           ├── 严重度
           ├── 趋势
           ├── 版本关联
           └── 建议动作
```

一个成熟的话题节点应该长这样：

```json
{
  "topic_id": "combat.balance.skill_trigger.001",
  "topic_name": "战法触发随机性导致玩家质疑战斗公平",
  "parent_topic": "战法与数值平衡",
  "dimension": ["策略可解释性", "战斗平衡", "公平感"],
  "definition": "玩家认为战法触发概率、战报表现或实际效果与预期不一致，导致对战斗结果产生不信任。",
  "include_rules": [
    "提到战法触发概率异常",
    "提到战报看不懂",
    "提到同阵容结果波动过大",
    "提到被低战力反杀且无法理解原因"
  ],
  "exclude_rules": [
    "单纯询问战法搭配攻略",
    "单纯反馈战报显示 BUG"
  ],
  "representative_entities": ["战法", "触发率", "战报", "控制", "伤害"],
  "common_intents": ["complaint", "balance_request", "explanation_request"],
  "affected_segments": ["midgame", "pvp_players", "lightpayer"],
  "severity_rule": "若伴随退游、退款、差评、社区扩散，则严重度提升",
  "owner": ["combat_designer", "system_designer", "community_ops"],
  "recommended_actions": [
    "增强战报解释",
    "补充战法概率说明",
    "检查异常战报样本",
    "评估高争议战法的触发/收益曲线"
  ]
}
```

这样的话题不是一个“聚类结果”，而是一个可维护的产品知识资产。

---

## 7. 关键分析视角：SLG 用户原声不能只看“负面量”

SLG 游戏的用户反馈有很强的阶段性和组织性。建议每个话题都至少带上以下切片：

### 7.1 玩家阶段

```text
新手期：引导、开荒、资源、看不懂、前期成就感
成长期：配将、武将获取、建筑升级、加入一门
冲突期：PVP、攻城、强弱差距、战报、兵损
赛季中后期：目标断层、霸业归属、弱盟流失、疲劳
赛季结算期：奖励、公平、转服、合服、回流
```

App Store 台湾区评论里能看到“新手引导顺、系统边玩边懂、前期不卡退坑、送的武将可用”等正向新手体验信号；而日本区评论回复里也出现了玩家同时认可一门协作与公平性、但对抽卡难度和一门/编成依赖强感到不满的典型混合反馈。([App Store][14])

### 7.2 玩家身份

```text
普通成员：资源、开荒、参与感
一门管理/当主：组织成本、活跃、外交、攻城排期
高战玩家：战斗平衡、赛季目标、强度焦虑
低战/无氪玩家：公平感、被碾压、可贡献空间
轻度玩家：时间压力、活动负担、错过焦虑
历史 IP 玩家：还原度、武将、剧情、沉浸
```

### 7.3 反馈性质

```text
体验阻塞：我玩不下去
认知阻塞：我看不懂
公平性质疑：我觉得不公平
目标缺失：我不知道接下来干什么
社交压力：我被组织要求绑架
数值争议：我觉得某武将/战法过强或过弱
技术问题：我无法稳定玩
情感表达：我喜欢/讨厌这个设计
```

---

## 8. 话题优先级评分模型

建议每个 Issue Node 不只统计数量，而是计算“行动优先级”。

可以先用一个简单模型：

```text
Priority =
  声量 × 严重度 × 增长趋势 × 高价值玩家权重 × 版本相关性 × 置信度
  ÷ 处理成本
```

字段设计：

```json
{
  "volume_7d": 384,
  "volume_30d": 1220,
  "trend_rate": 1.7,
  "severity_avg": 4.2,
  "negative_ratio": 0.82,
  "churn_signal_ratio": 0.19,
  "payer_ratio": 0.31,
  "newbie_ratio": 0.44,
  "version_correlation": 0.76,
  "confidence": 0.88,
  "suggested_priority": "P1"
}
```

特别建议增加三个 SLG 专属指标：

1. **组织扩散系数**：同一问题是否被多个一门/服务器共同提及。
2. **赛季节点敏感度**：是否集中出现在开服、破关、霸业、结算、合服等节点。
3. **流失表达率**：是否出现“退游、不玩了、卸载、转竞品、朋友都走了”。

---

## 9. 你们当前技术路线的改造建议

你们目前已经做到：

```text
用户原声 → LLM 小结 → 实体抽取 → 话题描述
```

我建议升级成：

```text
用户原声
  → 原子反馈拆分
  → 实体标准化
  → 意图/情绪/严重度/玩家阶段抽取
  → 领域本体挂载
  → 领域内聚类
  → LLM 生成话题节点
  → 人工审核
  → 稳定话题树 + 新兴话题池
```

最重要的变化有三个：

第一，**从“每条原声一条总结”改为“一条原声多条原子反馈”**。
SLG 玩家经常在一句话里同时反馈资源、武将、联盟、赛季和付费问题。

第二，**从“关键词实体”改为“规范实体 + 关系”**。
例如：

```text
核心武将 - 获取困难 - 阵容成型慢
弱一门 - 组织力不足 - 攻城失败
S2 - 强弱差距扩大 - 低战玩家流失
```

第三，**从“无监督聚类”改为“半监督层级聚合”**。
你们需要一个由策划定义的稳定骨架，同时允许模型发现新问题。

---

## 10. 推荐落地架构

```text
数据源层
  游戏内反馈 / 客服工单 / 问卷 / 社区 / App Store / Google Play / 巴哈 / Discord / Facebook

处理层
  清洗去重 / 语言检测 / 翻译或多语 embedding / 垃圾评论识别 / 版本和服务器补全

LLM 抽取层
  实体抽取 / 原子反馈拆分 / 意图识别 / 情绪识别 / 严重度 / 玩家阶段 / 证据句

知识层
  SLG 领域本体 / 武将别名表 / 战法表 / 兵种表 / 活动表 / 版本表 / 服务器表

聚合层
  Top-down 分类 / Bottom-up 聚类 / 主题命名 / 主题合并 / 新兴话题发现

审核层
  策划审核 / 运营审核 / 客服审核 / 标注修正 / 规则沉淀

分析层
  趋势看板 / 版本对比 / 玩家分群 / 赛季节点分析 / 竞品对比 / 行动建议

决策层
  数值调整 / 活动调整 / 公告解释 / 新手优化 / 商业化调整 / 技术修复 / 社区回应
```

工具组合可以是：

```text
Embedding: bge-m3 / text-embedding-3-large / multilingual-e5
Vector DB: Milvus / Qdrant / OpenSearch
Topic Modeling: BERTopic / HDBSCAN / Agglomerative Clustering
LLM: 结构化抽取 + 话题命名 + 代表样本总结
Annotation: Argilla / Label Studio / doccano
Graph: Neo4j / PostgreSQL + pgvector
Workflow: Airflow / Dagster / Prefect
Dashboard: Superset / Metabase / Grafana / 自研运营后台
```

---

## 11. 一个具体例子

原声：

> “S2 开始根本没法玩，我们小一门刚出生就被隔壁大盟堵死，抽不到强将，打城也没人指挥，准备退了。”

抽取后：

```json
{
  "raw_text": "S2 开始根本没法玩，我们小一门刚出生就被隔壁大盟堵死，抽不到强将，打城也没人指挥，准备退了。",
  "atomic_feedback": [
    {
      "claim": "S2 初期小一门被强盟压制导致无法发展",
      "entities": ["S2", "小一门", "强盟", "出生区域", "压制"],
      "intent": "complaint",
      "dimension": ["赛季生态健康度", "社交组织健康度"],
      "severity": 5
    },
    {
      "claim": "抽不到强将导致战斗竞争力不足",
      "entities": ["抽卡", "强将", "阵容强度"],
      "intent": "fairness_complaint",
      "dimension": ["付费公平感", "武将养成"],
      "severity": 4
    },
    {
      "claim": "攻城缺少指挥导致一门协作失败",
      "entities": ["攻城", "一门指挥", "组织协作"],
      "intent": "complaint",
      "dimension": ["社交组织健康度", "攻城体验"],
      "severity": 4
    },
    {
      "claim": "玩家表达退游倾向",
      "entities": ["退游"],
      "intent": "churn_signal",
      "dimension": ["留存风险"],
      "severity": 5
    }
  ]
}
```

向上聚合：

```text
原子反馈 1
 → S2 小一门出生区被强盟压制
 → 赛季早期强弱生态失衡
 → 赛季生态健康度 / 留存风险

原子反馈 2
 → 核心武将获取困难
 → 阵容成型压力
 → 付费公平感 / 武将养成

原子反馈 3
 → 攻城缺少组织指挥
 → 弱一门组织能力不足
 → 社交组织健康度 / 攻城体验

原子反馈 4
 → 明确退游表达
 → 高危流失信号
 → 留存风险
```

这才是对运营策划有价值的结构化洞察。

---

## 12. 我建议你们最终沉淀三类资产

**第一类：SLG 领域本体库**
包括系统、武将、战法、兵种、资源、活动、服务器、赛季、付费项、社交组织、BUG 类型、竞品词表。

**第二类：稳定话题树**
用于周报、版本复盘、运营看板。它应该稳定，不要每周自动大变。

**第三类：新兴话题发现池**
用于发现异常舆情、新版本问题、活动争议、突然爆发的 BUG、强盟生态事件。

最推荐的产品形态是：

```text
左侧：稳定话题树
中间：话题趋势、情绪、严重度、玩家分群
右侧：代表原声、证据链、关联版本/服务器/活动、建议动作
底部：相似历史问题、处理记录、上线后效果追踪
```

---

## 13. 最重要的设计原则

1. **不要让 LLM 总结替代原声证据**。总结用于聚合，原声用于决策可信度。
2. **不要只按文本相似聚类，要加入实体、系统、赛季、玩家阶段。**
3. **不要追求单标签，SLG 原声天然多标签。** 一条反馈可以同时属于“赛季生态、付费公平、一门组织、流失风险”。
4. **不要把声量等同优先级。** 少量高价值玩家、一门管理者、强社交传播节点的反馈，可能比大量泛吐槽更重要。
5. **稳定话题树和新兴话题发现要分开。** 前者服务看板，后者服务预警。
6. **每个话题节点必须有 include/exclude 规则。** 否则后期会出现话题漂移，聚类结果越来越不可控。
7. **运营分析最终要落到动作。** 每个话题都要能指向：解释、修复、补偿、调数值、改活动、优化引导、优化商业化、加强治理。

一句话总结：你们要构建的不是“用户评论聚类系统”，而是一个面向 SLG 长线运营的 **玩家诉求知识图谱 + 层级话题洞察系统**。这套系统的核心价值，是把海量原声从“玩家在抱怨什么”提升到“哪个系统、哪个阶段、哪类玩家、因为什么机制产生了什么行为风险，以及策划/运营应该优先处理什么”。

[1]: https://play.google.com/store/apps/details?hl=zh&id=com.sialiagames.xczyw.gp.tw "信長之野望 真戰 - Google Play 上的应用"
[2]: https://www.koeitecmoamerica.com/manual/nobunaga/awakening/en/3200.html "Game Outline | NOBUNAGA'S AMBITION: Awakening　Official Web Manual"
[3]: https://news.denfaminicogamer.jp/kikakuthetower/2512182z "『信長の野望 真戦』レビュー・評価・感想｜マルチプレイが楽しい壮大な戦国ごっこ遊び"
[4]: https://github.com/maartengr/bertopic "GitHub - MaartenGr/BERTopic: Leveraging BERT and c-TF-IDF to create easily interpretable topics. · GitHub"
[5]: https://maartengr.github.io/BERTopic/getting_started/hierarchicaltopics/hierarchicaltopics.html "Hierarchical Topic Modeling - BERTopic"
[6]: https://github.com/ArikReuter/TopicGPT "GitHub - ArikReuter/TopicGPT: TopicGPT allows to integrate the benefits of LLMs into Topic Modelling · GitHub"
[7]: https://github.com/dborrelli/chat-intents "GitHub - dborrelli/chat-intents: Clustering sentence embeddings to extract message intent · GitHub"
[8]: https://github.com/ddangelov/top2vec "GitHub - ddangelov/Top2Vec: Top2Vec learns jointly embedded topic, document and word vectors. · GitHub"
[9]: https://github.com/MaartenGr/KeyBERT "GitHub - MaartenGr/KeyBERT: Minimal keyword extraction with BERT · GitHub"
[10]: https://github.com/InstituteforDiseaseModeling/AIAugmentedSurveyResponseCategorization "GitHub - InstituteforDiseaseModeling/AIAugmentedSurveyResponseCategorization · GitHub"
[11]: https://github.com/LizeRaes/feedback-analyzer "GitHub - LizeRaes/feedback-analyzer · GitHub"
[12]: https://github.com/HumanSignal/label-studio "GitHub - HumanSignal/label-studio: Label Studio is a multi-type data labeling and annotation tool with standardized output format · GitHub"
[13]: https://github.blog/ai-and-ml/machine-learning/how-github-harnesses-ai-to-transform-customer-feedback-into-action/ "How GitHub harnesses AI to transform customer feedback into action - The GitHub Blog"
[14]: https://apps.apple.com/tw/app/%E4%BF%A1%E9%95%B7%E4%B9%8B%E9%87%8E%E6%9C%9B-%E7%9C%9F%E6%88%B0/id6739514639?platform=iphone&see-all=reviews "‎信長之野望 真戰 - 評分與評論 - App Store"
