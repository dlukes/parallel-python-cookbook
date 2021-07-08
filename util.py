def print_box(msg):
    assert "\n" not in msg
    xxxxx = "─" * (len(msg) + 2)
    print(
      "\r┌", xxxxx, "┐\n",
        "│ ", msg, " │\n",
        "└", xxxxx, "┘",
        sep=""
    )
