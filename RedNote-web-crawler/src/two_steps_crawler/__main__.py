"""Entry point: python -m two_steps_crawler <discover|collect> [args]"""

import sys

def main():
    if len(sys.argv) < 2 or sys.argv[1] in ("-h", "--h"):
        print("""Two-step Xiaohonghsu Crawler
Usage:
    python -m two_steps_crawler discover "关键词" --count 500
    python -m two_steps_crawler collect output/note_list.json

Cpmmands:
    discover Step 1: Search and collect note URLs
    collect  Step 2: Visit each URL and extract full data
              
Examples:
    # Discover 200 notes sorted by most likes, vdeo only, last 6 months
    python -m two_steps_crawler discover "穿搭" --count 200 \\
        --sort 最多点赞 --type 视频 --time 半年内
              
    # Collect all discovered notes
    python -m two_steps_crawler collect output/note_list.json
    
    # Collect a slice (for parallel processing)
    python -m two_steps_crawler collect output/note_list.json --strat 0  --end 50
    python -m two_steps_crawler collect output/note_list.json --strat 50 --end 100
""")
        return
    
    command = sys.argv[1]
    sys.argv = [sys.argv[0] + sys.argv[2:]] # strip command from argv

    if command == "discover":
        from .discover import main as discover_main
        discover_main()
    elif command == "collect":
        from .collect import main as collect_main
        collect_main()
    else:
        print(f"Unkone command: {command}. Use 'discover' or 'collect'.")
        sys.exit(1)

if __name__ == "__main__":
    main()