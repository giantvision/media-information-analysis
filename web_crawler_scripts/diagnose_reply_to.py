#!/usr/bin/env python3
"""
诊断脚本：抓取一个楼层的楼中楼子评论原始数据，
找出 "回复 某用户" 时 API 实际返回了哪些字段。

使用方式:
  python diagnose_reply_to.py <帖子ID> [楼层post_id]

如果不提供 post_id，会自动找到第一个有子评论的楼层。
"""

import re
import json
import time
import sys
import hashlib
import requests

SIGN_KEY = "tiebaclient!!!"
CLIENT_API_URL = "https://tiebac.baidu.com/c/f/pb/page"
CLIENT_FLOOR_API_URL = "https://tiebac.baidu.com/c/f/pb/floor"

CLIENT_COMMON_PARAMS = {
    "_client_type": "2",
    "_client_version": "12.57.1.0",
    "_os_version": "33",
    "_phone_imei": "000000000000000",
    "from": "tieba",
    "cuid": "baidutiebaapp",
}

CLIENT_HEADERS = {
    "User-Agent": "bdtb for Android 12.57.1.0",
    "Content-Type": "application/x-www-form-urlencoded",
    "Accept": "*/*",
    "Accept-Encoding": "gzip",
    "Connection": "keep-alive",
    "Host": "tiebac.baidu.com",
}


def calc_sign(params: dict) -> str:
    sorted_params = sorted(params.items())
    sign_str = ''.join(f"{k}={v}" for k, v in sorted_params)
    sign_str += SIGN_KEY
    return hashlib.md5(sign_str.encode('utf-8')).hexdigest()


def build_params(extra):
    params = dict(CLIENT_COMMON_PARAMS)
    params.update(extra)
    params['sign'] = calc_sign(params)
    return params


def find_floor_with_sub_comments(tid: str):
    """找到第一个有子评论的楼层"""
    params = build_params({
        "kz": tid, "pn": "1", "rn": "30", "lz": "0",
        "r": str(int(time.time())),
    })
    resp = requests.post(CLIENT_API_URL, data=params, headers=CLIENT_HEADERS, timeout=15)
    data = resp.json()

    post_list = data.get('post_list', [])
    for post in post_list:
        sub_count = int(post.get('sub_post_number', 0) or 0)
        if sub_count > 0:
            pid = str(post.get('id', ''))
            floor = int(post.get('floor', 0) or 0)
            print(f"找到第 {floor} 楼，post_id={pid}，子评论数={sub_count}")

            # 顺便打印内嵌的 sub_post_list 结构
            inline_sub = post.get('sub_post_list', {})
            if isinstance(inline_sub, dict):
                inline_posts = inline_sub.get('sub_post_list', [])
                if inline_posts:
                    print(f"\n--- 内嵌 sub_post_list 中的第一条子评论原始数据 ---")
                    print(json.dumps(inline_posts[0], ensure_ascii=False, indent=2))
                    # 检查是否有 "回复" 类型的
                    for sp in inline_posts:
                        content = sp.get('content', [])
                        if isinstance(content, list):
                            text_parts = [b.get('text', '') for b in content if isinstance(b, dict)]
                            full_text = ''.join(text_parts)
                            if '回复' in full_text:
                                print(f"\n--- 内嵌的 '回复' 子评论原始数据 ---")
                                print(json.dumps(sp, ensure_ascii=False, indent=2))
                                break

            return pid, floor
    return None, None


def dump_sub_comments(tid: str, pid: str):
    """抓取楼中楼子评论并打印原始 JSON"""
    params = build_params({
        "kz": tid, "pid": pid, "pn": "1", "rn": "20",
    })
    resp = requests.post(CLIENT_FLOOR_API_URL, data=params, headers=CLIENT_HEADERS, timeout=15)
    data = resp.json()

    print("\n" + "=" * 70)
    print("楼中楼 API (c/f/pb/floor) 完整响应的顶层键:")
    print(list(data.keys()))

    # 打印 user_list
    user_list = data.get('user_list', [])
    print(f"\nuser_list 长度: {len(user_list)}")
    if user_list:
        print("user_list 第一个用户的键:", list(user_list[0].keys()) if isinstance(user_list[0], dict) else "非字典")

    # 打印子评论
    subpost_list = data.get('subpost_list', [])
    if not subpost_list:
        subpost_list = data.get('data', {}).get('subpost_list', [])
    print(f"\nsubpost_list 长度: {len(subpost_list)}")

    # 找到包含 "回复" 的子评论
    reply_comments = []
    for sp in subpost_list:
        if not isinstance(sp, dict):
            continue
        # 检查 content 文本
        content = sp.get('content', [])
        if isinstance(content, list):
            text_parts = [b.get('text', '') for b in content if isinstance(b, dict)]
            full_text = ''.join(text_parts)
            if '回复' in full_text:
                reply_comments.append(sp)

    if reply_comments:
        print(f"\n找到 {len(reply_comments)} 条 '回复' 类型的子评论")
        for i, sp in enumerate(reply_comments[:3]):
            print(f"\n{'=' * 70}")
            print(f"回复子评论 #{i+1} 完整原始数据:")
            print("=" * 70)
            print(json.dumps(sp, ensure_ascii=False, indent=2))

            # 重点检查各个可能包含被回复者信息的字段
            print(f"\n--- 关键字段检查 ---")
            print(f"  title          = {repr(sp.get('title', 'KEY_NOT_FOUND'))}")
            print(f"  reply_to_id    = {repr(sp.get('reply_to_id', 'KEY_NOT_FOUND'))}")
            print(f"  reply_uid      = {repr(sp.get('reply_uid', 'KEY_NOT_FOUND'))}")
            print(f"  reply_to_user  = {repr(sp.get('reply_to_user', 'KEY_NOT_FOUND'))}")
            print(f"  replyUser      = {repr(sp.get('replyUser', 'KEY_NOT_FOUND'))}")
            print(f"  reply_user     = {repr(sp.get('reply_user', 'KEY_NOT_FOUND'))}")

            # author 字段
            author = sp.get('author', {})
            print(f"  author 类型     = {type(author).__name__}")
            if isinstance(author, dict):
                print(f"  author.keys()  = {list(author.keys())}")
                print(f"  author.id      = {repr(author.get('id', ''))}")
                print(f"  author.name    = {repr(author.get('name', ''))}")
                print(f"  author.name_show = {repr(author.get('name_show', ''))}")

            # content 块详情
            content = sp.get('content', [])
            if isinstance(content, list):
                print(f"\n  content 块数量 = {len(content)}")
                for j, block in enumerate(content):
                    if isinstance(block, dict):
                        print(f"    block[{j}]: type={block.get('type','')} "
                              f"text={repr(block.get('text','')[:50])} "
                              f"uid={repr(block.get('uid',''))} "
                              f"name={repr(block.get('name',''))} "
                              f"name_show={repr(block.get('name_show',''))}")

            # 所有非空字段
            print(f"\n  所有非空字段:")
            for k, v in sp.items():
                if v and k not in ('content', 'author', 'log_param'):
                    print(f"    {k} = {repr(v) if len(repr(v)) < 100 else repr(v)[:100] + '...'}")
    else:
        print("\n未找到 '回复' 类型的子评论")
        if subpost_list:
            print(f"\n第一条子评论的原始数据:")
            print(json.dumps(subpost_list[0], ensure_ascii=False, indent=2))

    # 保存完整响应
    output = f"floor_api_raw_{tid}_{pid}.json"
    with open(output, 'w', encoding='utf-8') as f:
        json.dump(data, ensure_ascii=False, fp=f, indent=2)
    print(f"\n完整原始响应已保存到: {output}")


def main():
    if len(sys.argv) < 2:
        print("用法: python diagnose_reply_to.py <帖子ID或URL> [楼层post_id]")
        sys.exit(1)

    tid_input = sys.argv[1].strip()
    match = re.search(r'/p/(\d+)', tid_input)
    tid = match.group(1) if match else tid_input

    if len(sys.argv) >= 3:
        pid = sys.argv[2]
    else:
        print(f"正在从帖子 {tid} 中查找有子评论的楼层...")
        pid, floor = find_floor_with_sub_comments(tid)
        if not pid:
            print("未找到有子评论的楼层")
            sys.exit(1)

    print(f"\n正在抓取帖子 {tid} 的楼层 pid={pid} 的子评论...")
    dump_sub_comments(tid, pid)


if __name__ == '__main__':
    main()