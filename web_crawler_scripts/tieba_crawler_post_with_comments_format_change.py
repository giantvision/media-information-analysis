import json
import re

def clean_content(content: str) -> str:
    """
    如果内容以"回复"开头，则只保留第一个冒号":"后面的有效内容。
    """
    if content.startswith("回复"):
        idx = content.find(":")
        if idx != -1:
            content = content[idx + 1:].strip()
    return content


def process_tieba_jsonl(input_path: str, output_path: str):
    results = []

    with open(input_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            try:
                post = json.loads(line)
            except json.JSONDecodeError as e:
                print(f"[WARN] JSON解析失败，跳过该行: {e}")
                continue

            post_id = post.get("post_id", "")
            post_url = post.get("post_url", "")
            comments = post.get("comments", [])

            for comment in comments:
                # ── 主评论 ──
                user_id   = comment.get("user", {}).get("user_id", "")
                post_time = comment.get("post_time", "")
                content   = comment.get("content", "").strip()

                content = clean_content(content)
                if content:
                    results.append({
                        "post_id":   post_id,
                        "post_url":  post_url,
                        "user_id":   user_id,
                        "post_time": post_time,
                        "content":   content,
                    })

                # ── 子评论 ──
                for sub in comment.get("sub_comments", []):
                    sub_user_id   = sub.get("user_id", "")
                    sub_post_time = sub.get("time", "")
                    sub_content   = sub.get("content", "").strip()

                    sub_content = clean_content(sub_content)
                    if sub_content:
                        results.append({
                            "post_id":   post_id,
                            "post_url":  post_url,
                            "user_id":   sub_user_id,
                            "post_time": sub_post_time,
                            "content":   sub_content,
                        })

    with open(output_path, "w", encoding="utf-8") as f:
        for record in results:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

    print(f"处理完成，共输出 {len(results)} 条评论记录 → {output_path}")


if __name__ == "__main__":
    INPUT_PATH  = "tieba_post_with_comments.jsonl"
    OUTPUT_PATH = "tieba_post_with_comments_format_change.jsonl"
    process_tieba_jsonl(INPUT_PATH, OUTPUT_PATH)