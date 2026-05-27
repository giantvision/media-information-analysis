# 昆虫世界 2D 生存游戏 — Godot 4 上手实操文档

> 类型：横版 2D 探索 + 生存 | 风格参考：《空洞骑士》+《Don't Starve》
> 核心调整：降低操作精度要求，把"硬核动作"换成"探索 + 资源管理 + 节奏感战斗"
> 目标读者：有基本编程经验、想做独立游戏的开发者

---

## 一、游戏定位与设计哲学（先想清楚再写代码）

### 1.1 你真正要做的是什么

《空洞骑士》之所以难，不是因为它是 Metroidvania，而是因为它的核心循环是**精准平台跳跃 + 帧级闪避**。如果你想降低操作要求，要做的不是"把空洞骑士变简单"，而是**改换核心循环**：

| 维度 | 空洞骑士 | 你的游戏（建议） |
|---|---|---|
| 核心张力 | 操作精准度 | 资源管理 + 探索决策 |
| 战斗 | 时机+位移+技能管理 | 简单点按 + 节奏，无连招 |
| 失败惩罚 | 重跑+丢失 geo | 仅丢失部分资源，无回收挑战 |
| 平台跳跃 | 大量帧级精度跳跃 | 可错过的"软"跳跃，不会即死 |
| 进度门槛 | 战斗技巧 | 装备 / 食物 / 工具 |

这意味着你的游戏更接近 **《Webbed》+《Stardew Valley》+ 轻度横版战斗** 的混合体，而不是纯粹的 Metroidvania。

### 1.2 昆虫世界的设计优势

昆虫题材天然契合"小角色、大世界"的叙事，且：

- **生态多样性**：蚂蚁、蜘蛛、甲虫、蜂、蝴蝶等天然提供敌人/NPC 阵营划分
- **资源体系自然**：花蜜、露水、蛛丝、菌类、腐叶都可以作为生存物资
- **环境层次**：地下、地表、植物层、空中天然分层，对应不同区域
- **比例叙事**：玩家是小昆虫，一片落叶就是一张地图，雨滴就是危险事件

### 1.3 玩法降难度的具体方法

- 把跳跃的"窗口期"放宽（coyote time + jump buffer，下面给代码）
- 战斗不要求时机闪避，改用"装备/buff"作为强度成长
- 死亡惩罚轻量化：丢失正在采集的资源即可，已存入"巢穴"的安全
- 用"昼夜节奏"代替"硬性 boss 关卡"——夜晚危险物变多，白天可以休整
- 地图不强制连续探索，用"传送点（蒲公英、菌丝传送）"降低跑图疲劳

---

## 二、技术选型与环境准备

### 2.1 为什么选 Godot 4

- 完全免费且 MIT 协议，独立开发无后顾之忧
- 2D 工具链是所有引擎里最好的（没有之一）
- 场景文件是文本，方便用 Git 管理，也方便用 AI 协作开发
- GDScript 类 Python，上手快；如果熟悉 C# 也可以用
- 编辑器内置 TileMap、动画编辑器、信号系统，省去大量第三方库

### 2.2 安装与版本选择

下载地址：godotengine.org

**版本建议**：使用最新稳定版的 Godot 4（撰文时为 4.x 主线）。**强烈建议跟 4.3+ 走**，TileMapLayer、Compositor 这些新功能后面会用到。Godot 3 已经不推荐用于新项目。

**.NET 还是 GDScript**：除非你已经精通 C#，否则直接用 GDScript 标准版。性能足够。

### 2.3 必装编辑器插件

可在 Godot 内的 AssetLib（资源库）里直接搜索：

| 插件 | 用途 | 说明 |
|---|---|---|
| **Metroidvania System (MetSys)** | 地图房间管理 | KoBeWi 出品，专为 Metroidvania 设计 |
| **Dialogue Manager** | 对话系统 | Nathan Hoad 出品，业内最常用 |
| **GodotSteam**（如要上 Steam） | Steam 平台对接 | 后期再装 |
| **AsepriteWizard** | Aseprite 工作流 | 美术用 Aseprite 时极大提效 |

### 2.4 推荐外部工具

- **Aseprite**（付费 $20，开源版可自编译）—— 像素美术的事实标准
- **LDtk** 或 Godot 内置 **TileMapLayer** —— 关卡编辑
- **Audacity**（免费）—— 音效编辑
- **Trello / Notion** —— 任务管理

---

## 三、项目结构规划

清晰的结构能在项目变大后救你的命。建议如下：

```
project/
├── addons/                  # 第三方插件
│   ├── MetroidvaniaSystem/
│   └── dialogue_manager/
├── assets/                  # 美术、音频原始资源
│   ├── sprites/
│   │   ├── player/
│   │   ├── enemies/
│   │   ├── items/
│   │   └── environment/
│   ├── audio/
│   │   ├── sfx/
│   │   └── music/
│   └── ui/
├── scenes/                  # 场景文件
│   ├── player/
│   │   ├── player.tscn
│   │   └── states/          # 状态机各状态
│   ├── enemies/
│   ├── items/
│   ├── world/               # 关卡场景
│   │   ├── garden/
│   │   ├── anthill/
│   │   └── pond/
│   ├── ui/
│   └── managers/            # 全局管理器场景
├── scripts/                 # 不依附于具体场景的脚本
│   ├── globals/             # Autoload 单例
│   ├── components/          # 可复用组件
│   └── data/                # 资源 Resource 类定义
├── resources/               # .tres 数据资源
│   ├── items/
│   ├── recipes/
│   └── enemy_stats/
└── project.godot
```

**核心原则**：场景文件（.tscn）和脚本文件（.gd）尽量同名同路径成对存放；可复用的数据用 `Resource` 类型存为 `.tres` 文件，便于在编辑器里调参。

---

## 四、核心系统实现

下面按"做出可玩 demo"的顺序来。每一节都给可直接复制使用的代码。

### 4.1 玩家控制器（带 Coyote Time 和 Jump Buffer）

这两个技巧是降低操作难度的"魔法武器"：玩家就算稍微提前按跳或刚走出平台后按跳，也能跳起来。几乎所有现代 2D 游戏都用。

新建 `scenes/player/player.tscn`，根节点为 `CharacterBody2D`，子节点：
- `AnimatedSprite2D`（或 `Sprite2D` + `AnimationPlayer`）
- `CollisionShape2D`
- `Camera2D`
- `Node` 命名为 `StateMachine`

**player.gd**：

```gdscript
extends CharacterBody2D
class_name Player

# --- 配置参数（在 Inspector 里可调） ---
@export var speed: float = 180.0
@export var jump_velocity: float = -380.0
@export var gravity: float = 980.0
@export var max_fall_speed: float = 600.0

# 玩家友好性参数
@export var coyote_time: float = 0.12      # 走下平台后还能跳的窗口
@export var jump_buffer_time: float = 0.15 # 提前按跳的有效窗口
@export var acceleration: float = 1500.0
@export var friction: float = 1200.0

# --- 内部状态 ---
var coyote_timer: float = 0.0
var jump_buffer_timer: float = 0.0
var was_on_floor: bool = false
var facing: int = 1  # 1 右，-1 左

# --- 生存属性 ---
@export var max_health: int = 5
@export var max_stamina: float = 100.0
var health: int
var stamina: float

@onready var sprite: AnimatedSprite2D = $AnimatedSprite2D

signal health_changed(new_value: int)
signal died

func _ready() -> void:
	health = max_health
	stamina = max_stamina

func _physics_process(delta: float) -> void:
	_update_timers(delta)
	_apply_gravity(delta)
	_handle_jump()
	_handle_movement(delta)
	_update_animation()
	
	was_on_floor = is_on_floor()
	move_and_slide()
	
	# 落地瞬间重置 coyote
	if not was_on_floor and is_on_floor():
		coyote_timer = coyote_time

func _update_timers(delta: float) -> void:
	if is_on_floor():
		coyote_timer = coyote_time
	else:
		coyote_timer -= delta
	
	if Input.is_action_just_pressed("jump"):
		jump_buffer_timer = jump_buffer_time
	else:
		jump_buffer_timer -= delta

func _apply_gravity(delta: float) -> void:
	if not is_on_floor():
		velocity.y = min(velocity.y + gravity * delta, max_fall_speed)

func _handle_jump() -> void:
	# 只有当 buffer 内按过跳跃 + coyote 内还能跳，才执行
	if jump_buffer_timer > 0 and coyote_timer > 0:
		velocity.y = jump_velocity
		jump_buffer_timer = 0
		coyote_timer = 0
	
	# 提前松开跳跃键时减弱跳跃高度（可变高度跳跃）
	if Input.is_action_just_released("jump") and velocity.y < 0:
		velocity.y *= 0.5

func _handle_movement(delta: float) -> void:
	var input_dir: float = Input.get_axis("move_left", "move_right")
	
	if input_dir != 0:
		velocity.x = move_toward(velocity.x, input_dir * speed, acceleration * delta)
		facing = sign(input_dir)
	else:
		velocity.x = move_toward(velocity.x, 0, friction * delta)

func _update_animation() -> void:
	sprite.flip_h = (facing == -1)
	if not is_on_floor():
		sprite.play("jump" if velocity.y < 0 else "fall")
	elif abs(velocity.x) > 10:
		sprite.play("walk")
	else:
		sprite.play("idle")

func take_damage(amount: int) -> void:
	health = max(0, health - amount)
	health_changed.emit(health)
	if health == 0:
		died.emit()
```

**Input Map 配置**：在 `项目 > 项目设置 > 输入映射` 里添加：
- `move_left` → A、左方向键
- `move_right` → D、右方向键
- `jump` → 空格、W、上方向键
- `attack` → J、左键
- `interact` → E
- `dash`（可选）→ Shift

### 4.2 战斗系统（无连招、强调反馈）

降低操作难度的关键是**"按一下就有大反馈"**，而不是要求"按对时机"。

**核心思路**：

1. 攻击是单次挥击，无连段
2. 攻击有大屏幕震动 + 击退 + 顿帧（hit pause）
3. 不设计精确闪避；用"短无敌时间"让任何受击都不会立即连死

在玩家场景下添加 `Area2D` 节点 `HitBox`，作为攻击判定：

```gdscript
# player_combat.gd（挂在 Player 节点上，或合并进 player.gd）
extends Node

@export var attack_damage: int = 1
@export var attack_duration: float = 0.25
@export var attack_cooldown: float = 0.4
@export var hit_stop_duration: float = 0.08  # 顿帧时长
@export var knockback_force: float = 300.0

@onready var player: Player = get_parent()
@onready var hit_box: Area2D = $"../HitBox"
@onready var hit_box_shape: CollisionShape2D = $"../HitBox/CollisionShape2D"

var is_attacking: bool = false
var attack_timer: float = 0.0

func _ready() -> void:
	hit_box_shape.disabled = true
	hit_box.body_entered.connect(_on_hit_box_body_entered)

func _process(delta: float) -> void:
	if attack_timer > 0:
		attack_timer -= delta
	if Input.is_action_just_pressed("attack") and not is_attacking and attack_timer <= 0:
		_perform_attack()

func _perform_attack() -> void:
	is_attacking = true
	attack_timer = attack_cooldown
	hit_box_shape.disabled = false
	# 根据玩家朝向翻转判定框
	hit_box.position.x = abs(hit_box.position.x) * player.facing
	
	await get_tree().create_timer(attack_duration).timeout
	hit_box_shape.disabled = true
	is_attacking = false

func _on_hit_box_body_entered(body: Node2D) -> void:
	if body.has_method("take_damage"):
		body.take_damage(attack_damage)
		_hit_stop()
		_apply_knockback(body)

func _hit_stop() -> void:
	# 顿帧：把整个游戏时间减速一下，制造打击感
	Engine.time_scale = 0.1
	await get_tree().create_timer(hit_stop_duration * 0.1).timeout
	Engine.time_scale = 1.0

func _apply_knockback(target: Node2D) -> void:
	if target is CharacterBody2D:
		var dir: Vector2 = (target.global_position - player.global_position).normalized()
		target.velocity = dir * knockback_force
```

**屏幕震动**：在 Camera2D 上加个小脚本，监听受击信号震动几帧即可。

### 4.3 简单敌人 AI（巡逻 + 警戒 + 追击）

不要一上来就写复杂行为树。简单的状态枚举对 99% 的 Metroidvania 敌人都够用。

**enemy_base.gd**：

```gdscript
extends CharacterBody2D
class_name Enemy

enum State { PATROL, ALERT, CHASE, ATTACK, HURT, DEAD }

@export var max_health: int = 3
@export var speed: float = 60.0
@export var detection_range: float = 200.0
@export var attack_range: float = 40.0
@export var damage_to_player: int = 1
@export var gravity: float = 980.0

var current_state: State = State.PATROL
var health: int
var direction: int = 1

@onready var sprite: AnimatedSprite2D = $AnimatedSprite2D
@onready var player_detector: RayCast2D = $PlayerDetector
@onready var wall_detector: RayCast2D = $WallDetector
@onready var floor_detector: RayCast2D = $FloorDetector

var player_ref: Player = null

func _ready() -> void:
	health = max_health
	# 通过 group 找玩家
	var players = get_tree().get_nodes_in_group("player")
	if players.size() > 0:
		player_ref = players[0]

func _physics_process(delta: float) -> void:
	if not is_on_floor():
		velocity.y += gravity * delta
	
	match current_state:
		State.PATROL: _state_patrol(delta)
		State.CHASE:  _state_chase(delta)
		State.ATTACK: _state_attack(delta)
		State.HURT:   pass  # 受击硬直，外部 timer 触发回到 patrol
		State.DEAD:   queue_free()
	
	move_and_slide()
	sprite.flip_h = (direction == -1)

func _state_patrol(delta: float) -> void:
	velocity.x = direction * speed
	# 撞墙或边缘掉头
	if wall_detector.is_colliding() or not floor_detector.is_colliding():
		direction *= -1
		wall_detector.target_position.x *= -1
		floor_detector.position.x *= -1
	
	if _player_in_range(detection_range):
		current_state = State.CHASE
		sprite.play("alert")

func _state_chase(delta: float) -> void:
	if not player_ref:
		current_state = State.PATROL
		return
	
	var dist: float = global_position.distance_to(player_ref.global_position)
	direction = sign(player_ref.global_position.x - global_position.x)
	velocity.x = direction * speed * 1.5
	
	if dist > detection_range * 1.5:
		current_state = State.PATROL
	elif dist < attack_range:
		current_state = State.ATTACK
		velocity.x = 0

func _state_attack(delta: float) -> void:
	velocity.x = 0
	# 这里触发攻击动画、生成攻击判定...
	# 攻击结束后回到 chase 或 patrol
	if not _player_in_range(attack_range * 1.2):
		current_state = State.CHASE

func _player_in_range(range: float) -> bool:
	if not player_ref: return false
	return global_position.distance_to(player_ref.global_position) < range

func take_damage(amount: int) -> void:
	health -= amount
	if health <= 0:
		_die()
	else:
		current_state = State.HURT
		sprite.play("hurt")
		await get_tree().create_timer(0.3).timeout
		current_state = State.CHASE

func _die() -> void:
	current_state = State.DEAD
	# 掉落物在这里生成
	sprite.play("die")
```

### 4.4 生存系统（饥饿 / 体力 / 体温）

生存类游戏的灵魂。建议用一个 Autoload 单例统一管理玩家长期属性。

**项目 > 项目设置 > 自动加载**，添加 `scripts/globals/player_stats.gd`：

```gdscript
extends Node

signal hunger_changed(value: float)
signal thirst_changed(value: float)
signal warmth_changed(value: float)
signal critical_state(stat_name: String)

@export var max_hunger: float = 100.0
@export var max_thirst: float = 100.0
@export var max_warmth: float = 100.0

var hunger: float = 100.0
var thirst: float = 100.0
var warmth: float = 100.0

# 衰减速率（每秒）—— 调参的关键
@export var hunger_decay: float = 0.4    # 250 秒一格满
@export var thirst_decay: float = 0.6
@export var warmth_decay: float = 0.2

func _process(delta: float) -> void:
	hunger = max(0, hunger - hunger_decay * delta)
	thirst = max(0, thirst - thirst_decay * delta)
	# warmth 在不同区域用不同速率，由区域脚本修改
	
	hunger_changed.emit(hunger)
	thirst_changed.emit(thirst)
	
	_check_critical()

func eat(amount: float) -> void:
	hunger = min(max_hunger, hunger + amount)
	hunger_changed.emit(hunger)

func drink(amount: float) -> void:
	thirst = min(max_thirst, thirst + amount)
	thirst_changed.emit(thirst)

func _check_critical() -> void:
	if hunger <= 0:
		critical_state.emit("starving")
	if thirst <= 0:
		critical_state.emit("dehydrated")
```

**衰减速率的调参经验**：让一个全满状态下的玩家在游戏的"舒适探索时长"（建议 8-15 分钟）能正常游玩，超过这个时间才感到压力。太快变成负担，太慢则形同虚设。

### 4.5 物品 / 采集 / 合成系统

用 Godot 的 `Resource` 类是最好的设计模式。

**item.gd**（自定义 Resource）：

```gdscript
extends Resource
class_name Item

@export var id: String = ""
@export var display_name: String = ""
@export var icon: Texture2D
@export_multiline var description: String = ""
@export var stack_size: int = 99
@export var item_type: ItemType = ItemType.MATERIAL

# 使用效果（食物、药剂用）
@export var hunger_restore: float = 0.0
@export var thirst_restore: float = 0.0
@export var health_restore: int = 0

enum ItemType { MATERIAL, FOOD, TOOL, EQUIPMENT, KEY_ITEM }
```

现在你可以在编辑器里**直接创建** `.tres` 文件：右键 res:// → 新建资源 → Item，命名为 `nectar_drop.tres`，填好属性。这样美术/策划同事也能改数据，不需要碰代码。

**inventory.gd**（Autoload 单例）：

```gdscript
extends Node

signal inventory_changed
signal item_added(item: Item, count: int)

const SLOT_COUNT: int = 20
var slots: Array[Dictionary] = []  # 每槽 {item: Item, count: int}

func _ready() -> void:
	for i in SLOT_COUNT:
		slots.append({"item": null, "count": 0})

func add_item(item: Item, count: int = 1) -> int:
	var remaining: int = count
	# 优先堆叠到现有槽
	for slot in slots:
		if remaining <= 0: break
		if slot.item == item and slot.count < item.stack_size:
			var add_amount = min(item.stack_size - slot.count, remaining)
			slot.count += add_amount
			remaining -= add_amount
	# 再放入空槽
	for slot in slots:
		if remaining <= 0: break
		if slot.item == null:
			var add_amount = min(item.stack_size, remaining)
			slot.item = item
			slot.count = add_amount
			remaining -= add_amount
	
	inventory_changed.emit()
	if count - remaining > 0:
		item_added.emit(item, count - remaining)
	return remaining  # 返回没塞下的数量

func remove_item(item: Item, count: int = 1) -> bool:
	var available = count_item(item)
	if available < count:
		return false
	var to_remove = count
	for slot in slots:
		if to_remove <= 0: break
		if slot.item == item:
			var take = min(slot.count, to_remove)
			slot.count -= take
			to_remove -= take
			if slot.count == 0:
				slot.item = null
	inventory_changed.emit()
	return true

func count_item(item: Item) -> int:
	var total = 0
	for slot in slots:
		if slot.item == item:
			total += slot.count
	return total
```

**合成配方**也用 Resource：

```gdscript
extends Resource
class_name Recipe

@export var inputs: Array[Item] = []
@export var input_counts: Array[int] = []
@export var output: Item
@export var output_count: int = 1
@export var required_workstation: String = ""  # "campfire", "anvil" 等

func can_craft() -> bool:
	for i in inputs.size():
		if Inventory.count_item(inputs[i]) < input_counts[i]:
			return false
	return true

func craft() -> bool:
	if not can_craft(): return false
	for i in inputs.size():
		Inventory.remove_item(inputs[i], input_counts[i])
	Inventory.add_item(output, output_count)
	return true
```

### 4.6 地图与房间系统

直接用 KoBeWi 的 **MetroidvaniaSystem (MetSys)** 插件最省事，它是开源的通用框架，主要面向 2D 网格化的 Metroidvania 游戏（横版或俯视都行），地图由方形/矩形网格上的房间组成，支持独立层（用于子区域、平行世界等），每个房间有自动生成的唯一 ID，配两个可选参数方法就能管理物体持久化。

简单流程：
1. AssetLib 装好后启用插件
2. 在 `项目 > 项目设置 > MetSys` 里画地图网格
3. 每个房间对应一个 `.tscn` 场景，关联到地图格子
4. 玩家走过通道时插件自动切场景

如果你想自己实现也可以，最小可行版本是：每个房间一个 `Node2D` 场景，房间出口处放 `Area2D`，触发时通过 `get_tree().change_scene_to_file(path)` 切换。但 MetSys 已经处理了路径连接、地图 UI、未探索区域标记，强烈推荐用它。

### 4.7 存档系统

最简单也最够用的方案：把所有需要存档的状态写进一个 Dictionary，存为 JSON。

```gdscript
extends Node
# 作为 Autoload，命名为 SaveSystem

const SAVE_PATH: String = "user://savegame.json"

func save_game() -> void:
	var data: Dictionary = {
		"version": 1,
		"player": {
			"position": [Game.player.global_position.x, Game.player.global_position.y],
			"health": Game.player.health,
			"current_scene": Game.current_scene_path,
		},
		"stats": {
			"hunger": PlayerStats.hunger,
			"thirst": PlayerStats.thirst,
			"warmth": PlayerStats.warmth,
		},
		"inventory": _serialize_inventory(),
		"world": Game.world_state,  # 已击败的 boss、已开启的存档点等
	}
	
	var file = FileAccess.open(SAVE_PATH, FileAccess.WRITE)
	file.store_string(JSON.stringify(data, "\t"))

func load_game() -> bool:
	if not FileAccess.file_exists(SAVE_PATH):
		return false
	var file = FileAccess.open(SAVE_PATH, FileAccess.READ)
	var data = JSON.parse_string(file.get_as_text())
	# ... 还原状态
	return true

func _serialize_inventory() -> Array:
	var result = []
	for slot in Inventory.slots:
		if slot.item:
			result.append({"id": slot.item.id, "count": slot.count})
		else:
			result.append(null)
	return result
```

存档触发时机：到达"安全点"（巢穴、储物箱）时自动存，避免玩家滥用存档 cheat 又不至于让人手动操作。

---

## 五、美术与音效资源

### 5.1 昆虫主题免费素材

**OpenGameArt.org**（CC0 / CC-BY）：

- **Ambient Pixel Art Insects** — Madame Berry 出品，8x8 像素的环境昆虫合集，作为 Patreon 公益项目放入公有领域，包含独立 PNG/GIF 和 spritesheet PSD
- **Old Bugs** by Master484 — CC0 公有领域的 bug sprite，无需署名，来自一款 BlitzBasic 写的 Bug Blaster 游戏

**itch.io 免费素材**：

- **Admurin's Insect Items** — 100 个手绘昆虫物品图标
- **Admurin's Flora and Fauna**（免费）
- **Animated insect enemy assets** — 直接可用的敌人动画
- **2D Pixel Art Bumble Bee / Dragonfly / Scarab Sprites**
- **Pixel Universe by Deirk** — 持续更新的昆虫像素合集

**通用平台素材包**（CC0）：

- **Kenney.nl** — 海量免费素材，质量稳定
- **Ansimuz Legacy Collection**（itch.io）— 16x16 整套像素 tileset
- **Sunny Land - Pixel Game Art Collection** — 可作森林/草地基底
- **Stylized Nature MegaKit**

### 5.2 音效与音乐

- **freesound.org** — CC0/CC-BY 海量音效，搜 "insect"、"forest ambient"
- **Pixabay Music** — 免费免署名背景乐
- **Incompetech**（Kevin MacLeod）— CC-BY，独立游戏常用
- **OpenGameArt 的 Music 分类** — 适合独立游戏体量

### 5.3 自己做美术的推荐流程

如果你想自己画：
1. **Aseprite** 画 sprite sheet（一行一个动作，每帧 16x16 或 32x32）
2. 用 AsepriteWizard 插件导入 Godot，自动切帧 + 生成 AnimationPlayer 资源
3. tileset 用 16x16 网格起步，后期不够再扩 32x32

---

## 六、开源参考项目（学他人之长）

### 6.1 Godot 2D 平台/Metroidvania 项目

**GDQuest 的开源 demo**：
- **godot-platformer-2d** — GDQuest 2019 Kickstarter 课程的 2D Metroidvania demo，包含跨多间房间的小世界、聚焦设计教学，github.com/gdquest-demos/godot-platformer-2d
- 他们其他 demo 也都开源，工程质量较高

**社区项目**：
- **steinj/Player-Controller-Godot** — 基于状态机的玩家控制器，实现 Idle/Run/Attack/Jump（含二段跳）/Fall/WallSlide/WallJump，含两种敌人：会追击的僵尸和射箭的弓箭手
- **ChrisMGeo/GodotHollowKnightController** — 在 Godot 里重制空洞骑士手感的玩家控制器，作为参考很好

**state machine 基础**：
- **godot-addons/godot-finite-state-machine** — 纯代码版 FSM，无编辑器插件，每个 state 可实现 _process/_physics_process/_input/_on_enter/_on_leave 回调
- The Shaggy Dev 的两篇博客（starter / advanced state machines in Godot 4）是必读

**Metroidvania 框架**：
- **KoBeWi/Metroidvania-System (MetSys)** — 上面提过，强烈推荐
- 论坛 forum.godotengine.org 上有一个 "Open source my metroidvania game" 帖子 完整开源了一个 metroidvania，包含状态机、行为树、设置系统、物品/地图等，去搜帖号 130228

### 6.2 商业级 Godot 2D 作品（可借鉴架构）

- **Lumencraft** by 2Dynamic Games — 一个 top-down 射击 + 基地建造 + 塔防的 Godot 商业游戏，他们用一套原创算法把 Godot 改造为支持像素级地形破坏。虽然类型不同，但工程实现可参考
- **Brotato**, **Dome Keeper**, **Cassette Beasts** —— 都是 Godot 商业成功的 2D 作品

### 6.3 跟你题材最接近的非 Godot 参考

- **Hollow Knight / Silksong** — 美术、节奏、关卡设计标杆
- **Webbed** — 蜘蛛主角的横版探索，操作友好版的昆虫世界
- **Bug Fables** — 昆虫题材 RPG，对世界观和角色设计有借鉴价值
- **Don't Starve / Stranded Deep** — 生存系统的核心循环参考
- **Stardew Valley** — 资源管理和"日夜节奏"的节奏感

### 6.4 学习资源

- **Godot 官方文档** — docs.godotengine.org（必读，质量极高）
- **GDQuest** — gdquest.com，付费课程含金量高
- **Heartbeast** YouTube 频道 — 2D 入门到进阶
- **The Shaggy Dev** — shaggydev.com，进阶模式与架构
- **HeartBeast** 的 Action RPG / Roguelike 教程系列

---

## 七、开发节奏建议（分阶段路线图）

千万不要一开始就想做完整的游戏。按下面 5 个阶段推进，每阶段都有可玩 build。

### 阶段 1：垂直切片（2-4 周）

目标：**一个房间、一个敌人、一种采集物**，但所有"游戏感"已经到位。

- [ ] 玩家可以移动、跳跃、攻击
- [ ] 一种敌人能巡逻、追击、受击、死亡
- [ ] 屏幕震动、顿帧、音效到位
- [ ] 一种可采集资源（比如花蜜）
- [ ] 一个 HUD 显示血量

这个阶段做完就该自我检验：**这个游戏感觉好玩吗？** 如果手感不对，先不要继续，回去调参。

### 阶段 2：核心循环（3-6 周）

- [ ] 5-8 种敌人（不同行为模式）
- [ ] 完整背包 + 合成系统
- [ ] 饥饿/口渴/体力衰减
- [ ] 一个"巢穴"作为存档/合成中心
- [ ] 3-5 间房间的小型世界

### 阶段 3：内容扩展（持续）

- [ ] 引入 MetSys 完整地图
- [ ] 多区域设计（地下、地表、植物层）
- [ ] 进阶装备（升级武器、护甲）
- [ ] Boss 战（即使简单，也要有"事件感"）

### 阶段 4：叙事与打磨（持续）

- [ ] 接入 Dialogue Manager
- [ ] 设计 NPC 与剧情
- [ ] 优化美术、音效一致性
- [ ] 教学关卡 / 引导

### 阶段 5：发布准备

- [ ] 完整存档系统
- [ ] 设置菜单（音量、按键、画面）
- [ ] 多平台测试（至少 Windows + Linux）
- [ ] Steam 页面或 itch.io 上线

---

## 八、常见陷阱与建议

### 8.1 工程层面

**不要过早抽象**。初学者经常一开始就写"通用的实体系统"、"通用的行为树"。在你有 3 个以上具体例子前，不要做抽象。先复制粘贴，等你看清模式再提炼。

**美术资源用占位图先做完玩法**。把所有玩法系统跑通之前，全用 Kenney 的免费方块和形状。等核心循环验证可玩，再考虑统一美术风格。这能省你几个月时间。

**版本控制是必须的**。从第一天就开 Git。Godot 4 的 .tscn 是文本，diff 友好。建议用 GitHub 私有库 + Git LFS 存大文件。

**.gitignore 必加**：
```
.godot/
*.tmp
.import/
```

### 8.2 设计层面

**降低操作要求，不等于降低挑战**。挑战可以来自资源管理、决策（这条路探索还是回家？）、信息不完全（地图上的未知区域）。把"难"的地方从"手"挪到"脑"。

**让玩家始终知道目标**。Metroidvania 最容易劝退的就是"我不知道该去哪"。即使你做的是开放探索，也至少给一个"主线指示"（比如 NPC 委托、地图标记的未探索特殊点）。

**测试要找非朋友玩家**。朋友会很客气；陌生人才会告诉你哪里不爽。

### 8.3 题材层面

**昆虫题材的潜在卖点**：

- 微观视角带来的"日常物品变奇观"——一滴露水可以是泉水，一片树叶可以是大陆
- 真实昆虫的生态行为可以变成 lore，让玩家有"我学到东西了"的奖励感
- 不同昆虫族群的政治/文化冲突天然适合做势力系统

**要避免的陷阱**：

- 别让昆虫都长得太像。设计阶段就要保证剪影辨识度
- 避开"令人不适的真实蜘蛛/蟑螂特写"——Hollow Knight 用拟人化避开了这点
- 蜜蜂、蚂蚁、瓢虫这些"友好型"昆虫已经被用滥；想想白蚁、蜉蝣、蝼蛄等冷门题材

### 8.4 性能小贴士

Godot 2D 性能瓶颈通常在：

- **过多的 `_process`/`_physics_process` 调用**——不需要的 Node 关掉 process
- **没必要的 Area2D**——能用 RayCast 就别用 Area
- **AnimatedSprite 帧数过多**——同一动作 6-8 帧就够，不需要 30 帧
- **TileMap 没用 LightOccluder**——光照计算昂贵

---

## 九、最后：一个不该忽略的"加分项"

如果你看了我之前的 Godot + LLM 分析，**昆虫世界 + 生存类是 LLM-NPC 的潜在好题材**。例如：

- 巢穴里的蚂蚁工头可以是 LLM 驱动的，根据玩家行为分配任务
- 商人甲虫可以根据玩家库存动态议价、嘲讽、给提示
- 你死亡后，幸存者会以你的故事为素材生成"传说"，下一周目会出现

如果你不打算第一版就上 LLM，至少在系统设计时**给 NPC 对话留接口**，把现在写死的对话树用 Dialogue Manager 包装，未来切换到 LLM 后端只需要替换数据源。NobodyWho（本地 LLM 推理插件）的存在让这一切都是可行的。

---

## 附录：核心代码文件清单

最小可玩原型需要的脚本：

| 文件 | 用途 | 优先级 |
|---|---|---|
| `player/player.gd` | 玩家控制器 | P0 |
| `player/player_combat.gd` | 攻击逻辑 | P0 |
| `enemies/enemy_base.gd` | 敌人基类 | P0 |
| `globals/player_stats.gd` (Autoload) | 生存属性 | P1 |
| `globals/inventory.gd` (Autoload) | 背包 | P1 |
| `globals/save_system.gd` (Autoload) | 存档 | P2 |
| `data/item.gd` (Resource) | 物品定义 | P1 |
| `data/recipe.gd` (Resource) | 配方定义 | P2 |
| `ui/hud.gd` | 状态显示 | P0 |
| `world/room_manager.gd` | 房间切换（或用 MetSys） | P1 |

P0 = 第 1 周必须；P1 = 第 1 个月内；P2 = 后续。

祝你做出一款好玩的虫虫小世界！🐛
