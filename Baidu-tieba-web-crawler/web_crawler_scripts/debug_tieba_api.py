#!/usr/bin/env python3
"""
诊断脚本：打印贴吧客户端 API 返回的原始数据结构
用于排查 user 信息为空的问题
"""
import hashlib
import json
import time
import requests

SIGN_KEY = "tiebaclient!!!"
CLIENT_API_URL = "https://tiebac.baidu.com/c/f/pb/page"
CLIENT_HEADERS = {
    "User-Agent": "bdtb for Android 12.57.1.0",
    "Content-Type": "application/x-www-form-urlencoded",
    "Accept": "*/*",
    "Connection": "keep-alive",
    "Host": "tiebac.baidu.com",
}
COMMON_PARAMS = {
    "_client_type": "2",
    "_client_version": "12.57.1.0",
    "_os_version": "33",
    "_phone_imei": "000000000000000",
    "from": "tieba",
    "cuid": "baidutiebaapp",
}

def calc_sign(params):
    sorted_params = sorted(params.items())
    sign_str = ''.join(f"{k}={v}" for k, v in sorted_params)
    sign_str += SIGN_KEY
    return hashlib.md5(sign_str.encode('utf-8')).hexdigest()

def main():
    tid = "10584752481"
    params = dict(COMMON_PARAMS)
    params.update({"kz": tid, "pn": "1", "rn": "5", "lz": "0", "r": str(int(time.time()))})
    params["sign"] = calc_sign(params)

    resp = requests.post(CLIENT_API_URL, data=params, headers=CLIENT_HEADERS, timeout=15)
    data = resp.json()

    # 保存完整响应到文件
    with open("debug_raw_response.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print("完整响应已保存到 debug_raw_response.json")

    print("\n" + "=" * 70)
    print("1. 顶层 keys:")
    print(f"   {list(data.keys())}")

    print("\n" + "=" * 70)
    print("2. user_list 信息:")
    user_list = data.get("user_list", "不存在")
    if user_list == "不存在":
        # 尝试 data.data.user_list
        user_list = data.get("data", {}).get("user_list", "不存在")
        if user_list != "不存在":
            print("   位置: data.data.user_list")
    else:
        print("   位置: data.user_list")

    if isinstance(user_list, list) and len(user_list) > 0:
        print(f"   数量: {len(user_list)}")
        print(f"   第一个 user 的 keys: {list(user_list[0].keys()) if isinstance(user_list[0], dict) else type(user_list[0])}")
        print(f"   第一个 user 完整内容:")
        print(f"   {json.dumps(user_list[0], ensure_ascii=False, indent=4)}")
    else:
        print(f"   值: {user_list}")

    print("\n" + "=" * 70)
    print("3. post_list 中第1条的 author 信息:")
    post_list = data.get("post_list", []) or data.get("data", {}).get("post_list", [])
    if post_list:
        first_post = post_list[0]
        print(f"   post keys: {list(first_post.keys())}")
        author = first_post.get("author", "不存在")
        author_id = first_post.get("author_id", "不存在")
        print(f"   post.author 类型: {type(author).__name__}, 值: {json.dumps(author, ensure_ascii=False) if isinstance(author, dict) else author}")
        print(f"   post.author_id: {author_id}")
        # 打印所有可能包含 user/author 的字段
        for k, v in first_post.items():
            if any(word in k.lower() for word in ["author", "user", "uid", "name", "portrait"]):
                print(f"   post.{k}: {v}")
    else:
        print("   post_list 为空!")

    print("\n" + "=" * 70)
    print("4. post_list 中第2条的 author 信息（如果有）:")
    if len(post_list) > 1:
        second = post_list[1]
        author = second.get("author", "不存在")
        author_id = second.get("author_id", "不存在")
        print(f"   post.author: {json.dumps(author, ensure_ascii=False) if isinstance(author, dict) else author}")
        print(f"   post.author_id: {author_id}")
        for k, v in second.items():
            if any(word in k.lower() for word in ["author", "user", "uid", "name", "portrait"]):
                print(f"   post.{k}: {v}")

    print("\n" + "=" * 70)
    print("5. thread.author 信息:")
    thread = data.get("thread", {}) or data.get("data", {}).get("thread", {})
    if thread:
        thread_author = thread.get("author", "不存在")
        print(f"   thread.author: {json.dumps(thread_author, ensure_ascii=False) if isinstance(thread_author, dict) else thread_author}")
        for k, v in thread.items():
            if any(word in k.lower() for word in ["author", "user", "uid"]):
                val_str = json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list)) else str(v)
                if len(val_str) > 200:
                    val_str = val_str[:200] + "..."
                print(f"   thread.{k}: {val_str}")

    print("\n" + "=" * 70)
    print(f"请将以上输出和 debug_raw_response.json 发给我分析！")

if __name__ == "__main__":
    main()